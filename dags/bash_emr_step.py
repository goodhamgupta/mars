# -*- coding: utf-8 -*-
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_add_steps_operator \
        import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['shubhamg2208@live.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

STEPS = [
    {
        "Name": "Hello World",
        "HadoopJarStep": {
            "Args": ["{}/hello_world.sh".format(Variable.get("bash_dir"))],
            "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
        },
        "ActionOnFailure": "CONTINUE"
    }
]

cluster_key = Variable.get('cluster_key', default_var=None)

dag = DAG(
    'bash_emr_step',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 8 * * *'
)


step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=cluster_key,
    aws_conn_id='aws_default',
    steps=STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=cluster_key,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

step_adder.set_downstream(step_checker)
