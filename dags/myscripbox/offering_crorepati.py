from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from common.operators.hive_emr_workflow_v2 import HiveEmrWorkflowV2
from hiveql.myscripbox.interim import offering_crorepati

DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 6),
    'email': ['shubham.gupta@scripbox.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

params = {
    'parent': 'msb_offering_crorepati',
    'child': 'msb_offering_crorepati_subdag',
    'snapshot_start': datetime.strptime("2019-07-21", "%Y-%m-%d"),
    'snapshot_end': datetime.strptime("2019-09-04", "%Y-%m-%d"),
    'start_date': DEFAULT_ARGS['start_date'],
    'schedule_interval': timedelta(days=1),
    'days': 7,
    'source_app': 'myscripbox',
    'snapshot_type': 'full',
    'hiveql_module': offering_crorepati,
    'table': 'offering_crorepati_incremental',
    'external_table': 'offering_crorepati',
    'registry_table': 'offering_crorepati_runs',
    'source_table': 'events'
}

main_dag = DAG(
  dag_id=params['parent'],
  schedule_interval=timedelta(days=1),
  start_date=datetime(2019, 9, 5)
)

sub_dag = SubDagOperator(
  subdag=HiveEmrWorkflowV2.create(params),
  task_id=params['child'],
  dag=main_dag
)

