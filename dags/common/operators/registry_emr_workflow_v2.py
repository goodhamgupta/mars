from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from common.operators.base_emr_workflow import BaseEmrWorkflow


class RegistryEmrWorkflowV2(BaseEmrWorkflow):
    """
    Class containing methods to access and alter information in the registry tables.
    """

    def __init__(self, params):
        self.child = params.get("child")
        self.stage_index = params.get("stage_index", 0)
        self.conn_id = params.get("conn_id", "airflow_db")
        self.conn_type = params.get("conn_type", "mysql")

    def _create_conn(self):
        """
        Function to create the connection to the DB which stores the registry table.
        """
        if self.conn_type == "mysql":
            return MySqlHook(mysql_conn_id=self.conn_id)
        else:
            return PostgresHook(postgres_conn_id=self.conn_id)

    def _fetch_tstamps_full(self, params):
        """
        Function to fetch the timestamps required for the full snapshot.

        :param params: Dict containing information for the full snapshot.
        :type params: dict

        :return (last_run, current_run): Timestamps signifying snapshot_start and snapshot_end datetime objects.
        :rtype: tuple
        """
        conn = self._create_conn()
        # Check for any pending executions.
        pending_one = conn.get_first(self._sql_lookup("REGISTRY_PENDINGS", params))

        if pending_one[0]:
            last_run = pending_one[0]
        else:
            last_run_records = conn.get_first(
                self._sql_lookup("REGISTRY_SELECT_MAX", params)
            )

            if not last_run_records or not last_run_records[0]:
                last_run = self._datetime_format(datetime.now() - timedelta(days=1))
            else:
                last_run = last_run_records[0]

        current_run = self._datetime_format(datetime.now())

        return (last_run, current_run)

    def _fetch_tstamps_incremental(self, params):
        """
        Function to fetch the timestamps required for the incremental snapshot.

        :param params: Dict containing information for the incremental snapshot.
        :type params: dict

        :return (last_run, current_run): Timestamps signifying snapshot_start and oeprator to fetch the snapshot_end from the source table using hiveql.
        :rtype: tuple(str, Operator)
        """
        stage_params = params.copy()
        dag = stage_params.get('dag')
        conn = self._create_conn()
        # Check for any pending executions.
        pending_one = conn.get_first(
            self._sql_lookup("REGISTRY_PENDINGS", stage_params)
        )

        if pending_one[0]:
            last_run = pending_one[0]
        else:
            last_run_records = conn.get_first(
                self._sql_lookup("REGISTRY_SELECT_MAX", stage_params)
            )

            if not last_run_records or not last_run_records[0]:
                last_run = self._datetime_format(datetime.now() - timedelta(days=1))
            else:
                last_run = last_run_records[0]

        stage_params.update({"hive_operator__return_value": True})

        timestamp_op = self._generate_emr_steps_jdbc(
            dag, "REGISTRY_SOURCE_LATEST_RUN", stage_params
        )

        return (self._datetime_format(last_run), timestamp_op)

    def _create_registry(self, params):
        """
        Function to check if the registry table exists. If not, it will be created.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        conn = self._create_conn()
        records = conn.get_first(self._sql_lookup("REGISTRY_EXIST", params))

        if not records:
            conn.run(self._sql_lookup("REGISTRY_CREATE", params))
        else:
            try:
                conn.run(self._sql_lookup("REGISTRY_ALTER", params))
            except Exception:
                # If the column already exists, don't do anything.
                pass

        return True

    def _insert_registry(self, params, **kwargs):
        """
        Function to insert the current timestamps into the registry table.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        conn = self._create_conn()
        stage_params = params.copy()
        conn.run(self._sql_lookup("REGISTRY_INSERT", params))

    def _insert_incremental(self, params, **kwargs):
        """
        Function to get registry timestamps for incremental runs.

        For fixing the timestamp problem, we will fetch the timestamps as follows:
        - The last_run_at timestamp for which succeded=1 will be the the current_run_at timestamp
        - For the current_run_at, we will fetch the maximum timestamp for the source table(i.e atomic.events) and use that INSTEAD of the current timestamp.
        - The advatange of using this method is:
            - It will always run for data that has been updated in the hive metastore.
            - In case the partitions are not being updated, the dag run will be skipped.

        :param params: Dict containing information for the snapshot
        :type params: dict
        """
        task_instance = kwargs.get('ti')
        if task_instance:
            snapshot_end = task_intance.xcom_pull(key='return_value', task_ids='registry_snapshot_end_0')
            stage_params = params.copy()
            stage_params.update({'snapshot_end': snapshot_end})
            insert_op = PythonOperator(
                task_id=f"registry_insert_0",
                python_callable=self._insert_registry,
                op_kwargs={"params": stage_params},
            )
            return (insert_op, stage_params)
        else:
            raise ValueError("Task instance not found. Force failing DAG.")


    def _update_registry(self, params):
        """
        Function to update the timestamps in the registry table if the DAG was successful.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        conn = self._create_conn()
        conn.run(self._sql_lookup("REGISTRY_SUCCEEDED", params))

    @classmethod
    def registry_create(cls, params):
        """
        Python operator to create the registry table.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        obj = cls(params)
        registry_create = PythonOperator(
            task_id=f"registry_create_{obj.stage_index}",

            python_callable=obj._create_registry,
            op_kwargs={"params": params},
        )

        return registry_create

    @classmethod
    def registry_insert(cls, params):
        """
        Python operator to insert into the registry table.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        obj = cls(params)
        registry_insert = PythonOperator(
            task_id=f"registry_insert_{obj.stage_index}",

            python_callable=obj._insert_registry,
            op_kwargs={"params": params},
        )
        return registry_insert

    @classmethod
    def registry_insert_incremental(cls, params):
        obj = cls(params)
        (registry_insert_incremental, updated_params) = PythonOperator(
            task_id=f"registry_snapshot_end_{obj.stage_index}",
            python_callable=obj._insert_incremental,
            op_args={"params": params},
        )
        return (registry_insert_incremental, updated_params)

    @classmethod
    def registry_update(cls, params):
        """
        Python operator to update the registry table.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        obj = cls(params)
        registry_update = PythonOperator(
            task_id=f"registry_update_{obj.stage_index}",

            python_callable=obj._update_registry,
            op_kwargs={"params": params},
        )

        return registry_update

    @classmethod
    def fetch_tstamps(cls, params):
        """
        Python operator to fetch the timestamps for the current snapshot.

        :param params: Dict containing information for the snapshot.
        :type params: dict
        """
        obj = cls(params)
        #if params.get("snapshot_type") == "full":
        #    last_run, current_run = obj._fetch_tstamps_full(params)
        #    return last_run, current_run
        #else:
        (last_run, latest_timestamp_op) = obj._fetch_tstamps_incremental(params)
        return (last_run, latest_timestamp_op)
