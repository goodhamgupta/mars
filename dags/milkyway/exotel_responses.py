# -*- coding: utf-8 -*-
# Airflow DAG for exotel responses
from operators.sqoop_emr_workflow import SqoopEmrWorkflow
from airflow import DAG


params = {
    'schedule_interval': '0 13 * * *',
    'source_app': 'milkyway',
    'destination': 'exotel_responses'
}

dag = SqoopEmrWorkflow.create(params)

