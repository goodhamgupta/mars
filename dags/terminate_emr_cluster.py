# -*- coding: utf-8 -*-

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.emr_terminate_job_flow_operator \
    import EmrTerminateJobFlowOperator


class MarsEmrTerminateJobFlowOperator(EmrTerminateJobFlowOperator):

    def execute(self, *args, **kwargs):

        try:
            cluster_key = Variable.get('cluster_key')
        except KeyError:
            self.log.info('There is no cluster to terminate!')
            return 0

        super(MarsEmrTerminateJobFlowOperator, self).execute(*args, **kwargs)

        # Delete the cluster id once it is terminated.
        Variable.delete('cluster_key')
        # Delete the cluster dns variable
        Variable.delete('cluster_dns')

DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['shubhamg2208@live.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


dag = DAG(
    'terminate_emr_cluster',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 20 * * *'
)

MarsEmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster_flow',
    job_flow_id=Variable.get('cluster_key', ''),
    aws_conn_id='aws_default',
    dag=dag
)
