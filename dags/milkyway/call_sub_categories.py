# -*- coding: utf-8 -*-
# Airflow DAG for call sub categories
from operators.sqoop_emr_workflow import SqoopEmrWorkflow
from airflow import DAG


params = {
    'schedule_interval': '0 13 * * *',
    'source_app': 'milkyway',
    'destination': 'call_sub_categories'
}

dag = SqoopEmrWorkflow.create(params)

