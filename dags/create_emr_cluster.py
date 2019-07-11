# -*- coding: utf-8 -*-
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator \
        import EmrCreateJobFlowOperator


class MarsEmrCreateJobFlowOperator(EmrCreateJobFlowOperator):

    def execute(self, *args, **kwargs):
        # Check if the cluster id is already exist.
        try:
            cluster_key = Variable.get('cluster_key')
        except KeyError:
            cluster_key = None
        self.log.info('Cluster already : %s', cluster_key)

        if cluster_key:
            self.log.info(
                'Cluster already running cluster_id: %s',
                cluster_key
            )
            return 0
        cluster_key = super(MarsEmrCreateJobFlowOperator, self).execute(
            *args, **kwargs
        )

        # Set the cluster id as variable.
        self.log.info(
            'Created new cluster cluster_id : %s',
            cluster_key
        )
        Variable.set('cluster_key', cluster_key)
        return cluster_key


DEFAULT_ARGS = {
        'owner': 'shubham',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(2),
        'email': ['shubham.gupta@scripbox.com'],
        'email_on_failure': False,
        'email_on_retry': False
        }


dag = DAG(
        'create_emr_cluster',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval='0 8 * * *'
        )

create_cluster = MarsEmrCreateJobFlowOperator(
        task_id='create_emr_cluster_flow',
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        dag=dag
        )
