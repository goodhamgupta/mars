# -*- coding: utf-8 -*-
# Airflow DAG for call tags
from operators.sqoop_emr_workflow import SqoopEmrWorkflow
from airflow import DAG


params = {
    'schedule_interval': '0 13 * * *',
    'source_app': 'milkyway',
    'destination': 'call_tags'
}

dag = SqoopEmrWorkflow.create(params)

