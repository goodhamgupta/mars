# -*- coding: utf-8 -*-

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator

DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['shubham.gupta@scripbox.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Take clister id backup.
cluster_id = Variable.get("cluster_id")
# Remove the cluster id from variables.
Variable.set("cluster_id", "")

dag = DAG(
    'terminate_emr_cluster',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 20 * * *'
)

EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster_flow',
    job_flow_id=cluster_id,
    aws_conn_id='aws_default',
    dag=dag
)

