from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from common.operators.hive_emr_workflow_v3 import HiveEmrWorkflowV3
from hiveql.myscripbox.interim import offering_source

DEFAULT_ARGS = {
    "owner": "shubham",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 5),
    "email": ["shubham.gupta@scripbox.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

params = {
    "parent": "msb_offering_source_v3",
    "child": "msb_offering_source_subdag",
    "default_args": DEFAULT_ARGS,
    "snapshot_start": datetime.strptime("2019-07-21", "%Y-%m-%d"),
    "snapshot_end": datetime.strptime("2019-08-04", "%Y-%m-%d"),
    "start_date": DEFAULT_ARGS["start_date"],
    "schedule_interval": None,
    "days": 7,
    "source_app": "myscripbox",
    "snapshot_type": "incremental",
    "hiveql_module": offering_source,
    "table": "offering_source_incremental",
    "external_table": "offering_source_v3",
    "registry_table": "offering_source_runs",
    "source_table": "events",
    "hive_operator__async": False,
}

main_dag = DAG(
    dag_id=params["parent"],
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2019, 8, 4),
)

sub_dag = SubDagOperator(
    subdag=HiveEmrWorkflowV3.create(params),
    task_id=params["child"],
    dag=main_dag
)
