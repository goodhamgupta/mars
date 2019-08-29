# -*- coding: utf-8 -*-
# Airflow DAG for investor_campaigns
from operators.sqoop_emr_workflow import SqoopEmrWorkflow
from airflow import DAG

params = {
    'schedule_interval': '0 13 * * *',
    'source_app': 'milkyway',
    'destination': 'investor_campaigns'
}

dag = SqoopEmrWorkflow.create(params)
