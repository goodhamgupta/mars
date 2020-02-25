# -*- coding: utf-8 -*-
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from common.operators.create_emr_hive_workflow import CreateEmrHiveWorkflow


params = {
    "default_args": {
        "owner": "shubham",
        "depends_on_past": False,
        "start_date": airflow.utils.dates.days_ago(2),
        "email": ["shubham.gupta@scripbox.com"],
        "email_on_failure": False,
        "email_on_retry": False,
    }
}

dag = CreateEmrHiveWorkflow.create(params)
