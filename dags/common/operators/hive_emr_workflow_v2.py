from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from common.operators.base_emr_workflow import BaseEmrWorkflow
from common.operators.registry_emr_workflow import RegistryEmrWorkflow

class HiveEmrWorkflowV2(BaseEmrWorkflow):
    """
    V2 of the Hive EMR Workflow
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
            schedule_interval=params.get('schedule_interval'),
            start_date=params.get('start_date')
        )

        (external_table_adder, external_table_checker) = self._generate_emr_steps(dag, "CREATE", params)
        (table_adder, table_checker) = self._generate_emr_steps(dag, "CREATE_TMP", params)
        (count_adder, count_checker) = self._generate_emr_steps(dag, "COUNT", params)
        registry_creater = RegistryEmrWorkflow.registry_create(params)

        registry_creater >> external_table_adder >> external_table_checker >> table_adder >> table_checker

        count_adder >> count_checker

        ops = [table_checker, count_adder]

        return {'dag': dag, 'ops': ops}

    def _create_dag(self, params, stage_params, base_dag_response):
        """
        Function to create and add all the downstream steps to the DAG.

        :param params: Dict containing information for the full snapshot.
        :type params: dict

        :param stage_params: Dict containing the stage parameters
        :type stage_params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """

        dag = base_dag_response.get('dag')
        [table_checker, count_adder] = base_dag_response.get('ops')

        dag_params = params.copy()
        dag_params.update(stage_params)

        registry_inserter = RegistryEmrWorkflow.registry_insert(dag_params)
        registry_updater = RegistryEmrWorkflow.registry_update(dag_params)

        (query_adder, query_checker) = self._generate_emr_steps(
            dag,
            "REPLICATE",
            dag_params
        )

        (insert_adder, insert_checker) = self._generate_emr_steps(
            dag,
            "INSERT",
            dag_params
        )

        (delete_adder, delete_checker) = self._generate_emr_steps(
            dag,
            "DELETE_TMP",
            dag_params
        )

        table_checker >> registry_inserter
        registry_inserter >> query_adder >> query_checker
        query_checker >> insert_adder >> insert_checker
        insert_checker >> delete_adder >> delete_checker
        delete_checker >> registry_updater
        registry_updater >> count_adder

        return True

    def _full_snapshot(self, params):
        """
        Function to perform full snapshot for the given parameters.

        :param params: Dict containing information for the full snapshot.
        :type params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """
        base_dag_response = self._create_base_dag(params)
        gen = self._get_dates(params)
        for index, (snapshot_start, snapshot_end) in enumerate(gen):
            stage_params = {
                'stage_name': f'full_snapshot_stage_{index}',
                'stage_index': index,
                'snapshot_start': snapshot_start,
                'snapshot_end': snapshot_end
            }
            self._create_dag(params, stage_params, base_dag_response)

        dag = base_dag_response.get('dag')
        return dag


    def _incremental_snapshot(self, params):
        """
        Function to perform incremental snapshot for the given parameters.

        :param params: Dict containing information for the incremental snapshot.
        :type params: dict

        :return dag: Airflow DAG containing all downstream steps for the snapshot.
        :rtype dag: Airflow DAG
        """
        base_dag_response = self._create_base_dag(params)
        (snapshot_start, snapshot_end) = RegistryEmrWorkflow.fetch_tstamps()
        stage_params = {
            'stage_name': f'incremental_snapshot_stage_{index}',
            'stage_index': 0,
            'snapshot_start': snapshot_start,
            'snapshot_end': snapshot_end
        }
        dag = self._create_dag(params, stage_params, base_dag_response)
        return dag

    @classmethod
    def create(cls, params):
        """ Function to create the sub dag to create derived tables in hive. Each sub dag has the following steps: - For a selected time range, we first fetch the data using a hive query
        - Store the result in a temporary table(This will be an EMR Step)
        - Write this temporary table to S3(EMR Step)
        - Monitor thestatus of the writes.(EMR Sensor)
        - Repeat this for all time slices.

        Because the time range is split into slices(usually of size 7 days), longer time ranges will have many EMR steps.
        Make sure the time ranges picked are short or you can afford to wait for a long time for all the steps to complete.
        """
        obj = cls()
        if params.get('snapshot_type') == "full":
            return obj._full_snapshot(params)
        else:
            return obj._incremental_snapshot(params)
