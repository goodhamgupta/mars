# -*- coding: utf-8 -*-
# Airflow DAG for campaign outcomes
from operators.sqoop_emr_workflow import SqoopEmrWorkflow
from airflow import DAG


params = {
    'schedule_interval': '0 13 * * *',
    'source_app': 'milkyway',
    'destination': 'campaign_outcomes'
}

dag = SqoopEmrWorkflow.create(params)

