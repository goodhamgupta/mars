import logging
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from common.operators.create_job_flow import MarsEmrCreateJobFlowOperator
from datetime import datetime, timedelta
from sb_emr.emr_client import EmrClient


class CreateEmrSparkWorkflow:
    """
    Class containing methods to create EMR cluster and
    initialize the spark session.
    """

    @classmethod
    def save_dns(cls, emr_client):
        """
        Save cluster dns as a variable
        """
        cluster_key = Variablee.get("cluster_key")
        cluster_dns = emr_client.get_public_dns(cluster_key)
        Variable.set("cluster_dns", cluster_dns)
        logging.log("Variable cluster_dns set")
        return True

    @classmethod
    def create(self, params):
        """
        Function to create the EMR cluster

        :param params: Dict containing DAG parameters
        :type dict

        :return: DAG object containing the steps to create cluster
        :rtype DAG
        """
        emr_client = EmrClient()
        dag = DAG(
            "create_emr_cluster",
            default_args=params.get("default_args"),
            schedule_interval=params.get("schedule_interval", "0 8 * * *")
        )

        create_cluster = MarsEmrCreateJobFlowOperator(
            task_id="create_emr_cluster_flow",
            aws_conn_id="aws_default",
            emr_conn_id="emr_default",
            dag=dag,
        )
        fetch_cluster_dns = PythonOperator(
            task_id="fetch_cluster_dns",
            python_callable=cls.save_dns,
            op_kwargs={"emr_client": emr_client},
            dag=dag,
        )
        create_cluster >> fetch_cluster_dns
        return dag
