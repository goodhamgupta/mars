# -*- coding: utf-8 -*-
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator \
        import EmrCreateJobFlowOperator

DEFAULT_ARGS = {
        'owner': 'shubham',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(2),
        'email': ['shubham.gupta@scripbox.com'],
        'email_on_failure': False,
        'email_on_retry': False
        }


def store(**kwargs):
    cluster_id = kwargs["cluster_id"]
    Variable.set("cluster_id", cluster_id)

dag = DAG(
        'create_emr_cluster',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval='0 3 * * *'
        )

create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster_flow',
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        dag=dag
        )

store_cluster_id = PythonOperator(
        task_id='store_cluster_id',
        bash_command='echo "{{ task_instance.xcom_pull("create_emr_cluster_flow")}}"',
        dag=dag,
        python_callable=store,
        op_kwargs={"cluster_id": '{{ task_instance.xcom_pull("create_emr_cluster_flow")}}'}
        )

create_cluster.set_downstream(store_cluster_id)
