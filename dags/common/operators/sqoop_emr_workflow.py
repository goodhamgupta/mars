from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


class SqoopEmrWorkflow:
    """
    Class specifying the EMR workflow using the steps API for the Sqoop import. Currently, we support only the full snapshot mode.
    """
    def __init__(self, params):
        self.cluster_key = Variable.get('cluster_key', 'dummy_cluster')
        self.source_app = params.get('source_app', 'milkyway')
        self.connection_name = params.get('connection_name', 'prod_mysql_milkyway')
        self.script = params.get('script', 'mysql_import.sh')
        self.destination = params.get('destination', None)
        self.default_args = {
            'owner': 'shubham',
            'depends_on_past': False,
            'start_date': datetime.now(),
            'email': ['shubham.gupta@scripbox.com'],
            'email_on_failure': False,
            'email_on_retry': False
        }


    def _create_dag(self, params):
        connection = BaseHook.get_connection(self.connection_name)

        arguments = [
            f"{Variable.get('sqoop_dir')}/{self.script}",
            connection.host,
            f"{connection.port}",
            connection.schema,
            connection.login,
            self.destination,
            f"{Variable.get('milkyway_sqoop_dest_dir')}/{self.destination}"
        ]

        steps = [
            {
                "Name": f"Sqoop {self.source_app} {self.destination} import",
                "HadoopJarStep": {
                    "Args": arguments,
                    "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                },
                "ActionOnFailure": "CONTINUE"
            }
        ]

        dag_name = f"{self.source_app}_sqoop_{self.destination}_import"

        dag = DAG(
            dag_name,
            default_args=self.default_args,
            dagrun_timeout=timedelta(hours=2),
            schedule_interval=params['schedule_interval']
        )

        adder = EmrAddStepsOperator(
            task_id='add_steps',
            job_flow_id=self.cluster_key,
            aws_conn_id='aws_default',
            steps=steps,
            dag=dag
        )

        checker = EmrStepSensor(
            task_id='watch_step',
            job_flow_id=self.cluster_key,
            step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            dag=dag
        )
        adder.set_downstream(checker)
        return dag

    @classmethod
    def create(cls, params):
        obj = cls(params)
        dag = obj._create_dag(params)
        return dag
