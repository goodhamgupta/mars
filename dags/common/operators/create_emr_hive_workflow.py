import logging
from datetime import datetime, timedelta

from airflow import settings
from airflow.models import DAG, Variable, Connection
from airflow.operators.python_operator import PythonOperator

from .create_job_flow_operator import MarsEmrCreateJobFlowOperator
from ..clients import EmrClient


class CreateEmrHiveWorkflow:
    """
    Class containing methods to create EMR cluster and
    initialize the spark session.
    """

    def _save_dns(*args, **kwargs):
        """
        Save cluster dns as a variable
        """
        emr_client = args[1]
        ti = kwargs["ti"]
        cluster_key = ti.xcom_pull(task_ids="create_cluster")
        cluster_dns = emr_client.get_cluster_dns(cluster_key)
        Variable.set("cluster_dns", cluster_dns)
        logging.info("Variable cluster_dns set")
        return True

    def _wait_for_completion(*args, **kwargs):
        """
        Wait for cluster to be ready for jobs.
        """
        emr_client = args[1]
        ti = kwargs["ti"]
        cluster_id = ti.xcom_pull(task_ids="create_cluster")
        emr_client.wait_for_cluster_creation(cluster_id)

    def _create_hive_connection(*args, **kwargs):
        """
        Function to create the hive connection. This connection will allow us to execute hive queries on the EMR cluster directly without using the steps API.
        """
        cluster_dns = Variable.get("cluster_dns")
        hive_conn = Connection(
            conn_id=f"hive_connection",
            login="hadoop",
            host=cluster_dns if cluster_dns else None,
        )

        session = settings.Session()
        session.add(hive_conn)
        session.commit()

    @classmethod
    def create(cls, params):
        """
        Function to create the EMR cluster

        :param params: Dict containing DAG parameters
        :type dict

        :return: DAG object containing the steps to create cluster
        :rtype DAG
        """
        obj = cls()
        emr_client = EmrClient()

        dag = DAG(
            "create_emr_cluster",
            default_args=params.get("default_args"),
            schedule_interval=params.get("schedule_interval", "0 8 * * *"),
        )

        create_cluster = MarsEmrCreateJobFlowOperator(
            task_id="create_cluster",
            aws_conn_id="aws_default",
            emr_conn_id="emr_default",
            dag=dag,
        )
        wait_for_creation = PythonOperator(
            task_id="wait_for_creation",
            python_callable=obj._wait_for_completion,
            provide_context=True,
            op_args=[emr_client],
            dag=dag,
        )
        fetch_dns = PythonOperator(
            task_id="fetch_dns",
            python_callable=obj._save_dns,
            provide_context=True,
            op_args=[emr_client],
            dag=dag,
        )

        create_hive_connection = PythonOperator(
            task_id="create_hive_connection",
            python_callable=obj._create_hive_connection,
            provide_context=True,
            dag=dag,
        )
        create_cluster >> wait_for_creation >> fetch_dns
        fetch_dns >> create_hive_connection
        return dag
