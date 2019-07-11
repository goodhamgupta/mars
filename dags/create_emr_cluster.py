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
        cluster_id = Variable.get('cluster_id')
        self.log.info('Cluster already : %s', cluster_id)

        if cluster_id:
            self.log.info(
                'Cluster already running cluster_id: %s',
                cluster_id
            )
            return 0
        # cluster_id = super(MarsEmrCreateJobFlowOperator, self).execute(
        #     *args, **kwargs
        # )

        # Set the cluster id as variable.
        self.log.info(
            'Created new cluster cluster_id : %s',
            cluster_id
        )
        Variable.set('cluster_id', cluster_id)
        return cluster_id


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
