from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from common.operators.base_emr_workflow import BaseEmrWorkflow
from common.operators.registry_emr_workflow_v2 import RegistryEmrWorkflowV2


class HiveEmrWorkflowV3(BaseEmrWorkflow):
    """
    V3 of the Hive EMR Workflow
    """

    def __init__(self):
        pass

    def _create_base_dag(self, params):
        """
        Function to create the base DAG which will contain the starting ops.

        :param params: Dict containing information for the full snapshot.
        :type params: dict
        """
        dag = DAG(
            f"{params.get('parent')}.{params.get('child')}",
            default_args=params.get("default_args"),
            schedule_interval=params.get("schedule_interval"),
            start_date=params.get("start_date"),
        )

        external_table_op = self._generate_emr_steps_jdbc(dag, "CREATE", params)
        table_adder_op = self._generate_emr_steps_jdbc(dag, "CREATE_TMP", params)
        count_adder_op = self._generate_emr_steps_jdbc(dag, "COUNT", params)

        registry_creater = RegistryEmrWorkflowV2.registry_create(params)

        registry_creater >> external_table_op >> table_adder_op

        ops = [table_adder_op, count_adder_op]

        return {"dag": dag, "ops": ops}

    def _create_core_dag(self, dag, params):
        query_op = self._generate_emr_steps_jdbc(dag, "REPLICATE", params)

        insert_op = self._generate_emr_steps_jdbc(dag, "INSERT", params)

        delete_op = self._generate_emr_steps_jdbc(dag, "DELETE_TMP", params)
        query_op >> insert_op >> delete_op

        return (query_op, delete_op)

    def _create_dag_incremental(self, params, stage_params, base_dag_response):
        """
        Function to create and add all the downstream steps to the DAG.

        :param params: Dict containing information for the full snapshot.
        :type params: dict

        :param stage_params: Dict containing the stage parameters
        :type stage_params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """

        dag = base_dag_response.get("dag")
        [table_adder_op, count_op] = base_dag_response.get("ops")

        dag_params = params.copy()
        dag_params.update(stage_params)

        latest_timestamp_op = dag_params.get("latest_timestamp_op")

        registry_inserter = RegistryEmrWorkflowV2.registry_insert(dag_params)
        registry_updater = RegistryEmrWorkflowV2.registry_update(dag_params)

        (query_op, delete_op) = self._create_core_dag(dag, dag_params)

        table_adder_op >> latest_timestamp_op
        latest_timestamp_op >> registry_inserter >> query_op
        delete_op >> registry_updater
        registry_updater >> count_op

        return dag

    def _full_snapshot(self, params):
        """
        Function to perform full snapshot for the given parameters.

        :param params: Dict containing information for the full snapshot.
        :type params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """
        raise NotImplementedError(
            "Full snapshot not available in V3. Please use V2 to get full snapshot and V3 to get increment snapshots."
        )

    def _incremental_snapshot(self, params):
        """
        Function to perform incremental snapshot for the given parameters.

        :param params: Dict containing information for the incremental snapshot.
        :type params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """
        base_dag_response = self._create_base_dag(params)
        stage_params = params.copy()
        stage_params.update({"dag": base_dag_response.get("dag")})

        (snapshot_start, latest_timestamp_op) = RegistryEmrWorkflowV2.fetch_tstamps(
            stage_params
        )

        stage_params.update(
            {
                "stage_name": "incremental_snapshot_stage",
                "snapshot_start": snapshot_start,
                "latest_timestamp_op": latest_timestamp_op,
            }
        )

        dag = self._create_dag_incremental(params, stage_params, base_dag_response)
        return dag

    @classmethod
    def create(cls, params):
        """
        Function to create the sub dag to create derived tables in hive

        :param params: Dict containing information for the snapshot snapshot.
        :type params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """
        obj = cls()
        snapshot_type = params.get("snapshot_type")
        if snapshot_type == "full":
            return obj._full_snapshot(params)
        elif snapshot_type == "incremental":
            return obj._incremental_snapshot(params)
        else:
            raise ValueError(
                "Invalid snapshot_type argument. It should be one of the following: full, incremental"
            )
