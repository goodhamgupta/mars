from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from common.operators.hive_emr_workflow import HiveEmrWorkflow


DEFAULT_ARGS = {
    'owner': 'shubham',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 11),
    'email': ['shubham.gupta@scripbox.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

params = {
    'parent': 'myscripbox_offering_source_table_create',
    'child': 'myscripbox_offering_source_table_split',
    'snapshot_start': datetime.strptime("2019-06-01", "%Y-%m-%d"),
    'snapshot_end': datetime.strptime("2019-07-22", "%Y-%m-%d"),
    'start_date': DEFAULT_ARGS['start_date'],
    'schedule_interval': timedelta(days=1),
    'days': 7,
    'source_app': 'myscripbox',
    'snapshot_type': 'full',
    'hiveql_module': 'offering_source',
    'destination': 'offering_source'
}

main_dag = DAG(
  dag_id=params['parent'],
  schedule_interval=timedelta(days=1),
  start_date=datetime(2019, 7, 11)
)

sub_dag = SubDagOperator(
  subdag=HiveEmrWorkflow.create(params),
  task_id=params['child'],
  dag=main_dag
)

