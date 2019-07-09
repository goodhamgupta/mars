# -*- coding: utf-8 -*-
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['shubham.gupta@scripbox.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

cluster_id = Variable.get('cluster_id', default_var=None)

connection = BaseHook.get_connection('prod_mysql_milkyway')

arguments = [
    f"{Variable.get('sqoop_dir')}/mysql_import.sh",
    connection.host,
    f"{connection.port}",
    connection.schema,
    connection.login,
    "call_sub_categories",
    f"{Variable.get('milkyway_sqoop_dest_dir')}/call_sub_categories"
]

STEPS = [
    {
        "Name": "Sqoop milkyway call sub categories import",
        "HadoopJarStep": {
            "Args": arguments,
            "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
        },
        "ActionOnFailure": "CONTINUE"
    }
]

dag = DAG(
    'milkyway_sqoop_call_sub_categories_import',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 13 * * *'
)


step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=cluster_id,
    aws_conn_id='aws_default',
    steps=STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=cluster_id,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

step_adder.set_downstream(step_checker)
