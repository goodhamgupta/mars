from typing import Dict, List, Tuple
from datetime import datetime, timedelta

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks import MySqlHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from .hive_emr_operator import HiveEmrOperator


class BaseEmrWorkflow:
    """
    Base classes specifying important functions for the EMR workflow
    """

    SNAPSHOT_TYPE_QUERIES = ["INSERT", "DELETE", "PURGE", "REPLICATE"]

    @staticmethod
    def _fetch_cluster_key() -> str:
        """
        Function to fetch the cluster_key stored in airflow variables.
        """
        return Variable.get("cluster_key", "dummy_cluster")

    @staticmethod
    def _fetch_hive_interim_dir(params: Dict[str, str]):
        source_app = params.get("source_app", "snowplow")
        return Variable.get(f"{source_app}_hive_interim_dir")

    @staticmethod
    def _fetch_hive_cli_dir() -> str:
        """
        Returns location of file in S3 containing bash script to execute hive command
        """
        return f"{Variable.get('hive_dir')}/hive_cli_expression.sh"

    @staticmethod
    def _datetime_format(obj):
        """
        Function to format date to the format: YYYY-MM-DD.

        :param obj: Datetime object
        :type obj: datetime

        :return: Formatted datetime string.
        :rtype: string
        """
        return obj.strftime("%Y-%m-%d %H-%M-%S")

    def _get_dates(self, params: Dict[str, str]) -> List[Tuple[str, str]]:
        """
        Function to generate chunks of date ranges.

        :param params: Dictionary containing the arguments needed to patch the SQL query
        :type params: dict

        :return: Generator containing tuples which are chunks seperated by specified days.
        :rtype: generator
        """
        snapshot_start = params.get("snapshot_start")
        snapshot_end = params.get("snapshot_end")
        days = params.get("days")
        result = []
        while snapshot_start <= snapshot_end:
            new = snapshot_start + timedelta(days=days)
            if new > snapshot_end:
                if days != 1:
                    result.append((self._datetime_format(snapshot_start),self._datetime_format(snapshot_end)))
                break
            else:
                result.append(
                    (self._datetime_format(snapshot_start), self._datetime_format(new))
                )
                snapshot_start = new
        return result

    def _sql_lookup(self, query: str, params: Dict[str, str]) -> str:
        """
        Method to identify and patch the query with the respective arguments.

        :param query: Type of query
        :type query: string

        :param params: Dictionary containing the arguments needed to patch the SQL query
        :type params: dict

        :return: Patched SQL statement.
        :rtype: string
        """
        sql_params = params.copy()
        location = self._fetch_hive_interim_dir(sql_params)
        external_table = sql_params["external_table"]
        sql_params["external_table_location"] = f"{location}/{external_table}"
        return getattr(params["hiveql_module"], query).format(**sql_params)

    def _generate_submission_steps(
        self, query: str, step_arguments: List[str], params: Dict[str, str]
    ) -> List[Dict[str, str]]:
        """
        Function to generate the steps that are submitted to EMR as a Hadoop step. This step is executed
        using the script-runner.jar which is available in EMR.

        """
        stage_name = params.get("stage_name", "snapshot_stage")
        name = f"{stage_name} {params.get('child')} {query}"
        if query in self.SNAPSHOT_TYPE_QUERIES:
            name = (
                name
                + f": Timerange {params['snapshot_start']} to {params['snapshot_end']}"
            )
        steps = [
            {
                "Name": name,
                "HadoopJarStep": {
                    "Args": step_arguments,
                    "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                },
                "ActionOnFailure": "CONTINUE",
            }
        ]

        return steps

    def _generate_emr_steps(self, dag, query, params):
        """
        Function to generate the steps needed for the EMR flow.

        :param dag: Airflow DAG object to which the steps will be added downstream.
        :type dag: Airflow DAG

        :param query: Type of SQL query
        :type query: string

        :param params: Dict containing information needed to add steps to the DAG.
        :type params: dict

        :return: Airflow DAG object containing the adder and checker steps.
        :rtype: Tuple containing the adder and watcher operators.
        """
        cluster_key = self._fetch_cluster_key()
        step_arguments = [self._fetch_hive_cli_dir(), self._sql_lookup(query, params)]

        snapshot_steps = self._generate_submission_steps(query, step_arguments, params)
        stage_index = params.get("stage_index", 0)
        adder_task_id = f"{query}_{stage_index}"
        watcher_task_id = f"{query}_watch_{stage_index}"

        adder = EmrAddStepsOperator(
            task_id=adder_task_id,
            job_flow_id=cluster_key,
            aws_conn_id="aws_default",
            trigger_rule="none_failed",
            steps=snapshot_steps,
            dag=dag,
        )

        watcher = EmrStepSensor(
            task_id=watcher_task_id,
            job_flow_id=cluster_key,
            step_id="{{ task_instance.xcom_pull(task_ids="
            + f"'{adder_task_id}'"
            + ",key='return_value')[0] }}",
            trigger_rule="none_failed",
            aws_conn_id="aws_default",
            dag=dag,
        )

        return (adder, watcher)

    def _generate_emr_steps_jdbc(self, dag, query, params):
        """
        Function to generate the steps needed for the EMR flow using the jdbc driver

        :param dag: Airflow DAG object to which the steps will be added downstream.
        :type dag: Airflow DAG

        :param query: Type of SQL query
        :type query: string

        :param params: Dict containing information needed to add steps to the DAG.
        :type params: dict

        :return: Airflow DAG object containing the jdbc operator
        :rtype: JDBC operator
        """

        stage_index = params.get("stage_index", 0)
        task_id = f"{query}_jdbc_{stage_index}"
        stage_params = params.copy()
        hql = self._sql_lookup(query, stage_params)
        stage_params.update({"hive_operator__query": hql})
        jdbc_op = HiveEmrOperator(
            task_id=task_id, trigger_rule="none_failed", params=stage_params, dag=dag
        )

        return jdbc_op
