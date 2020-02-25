from airflow.models import Variable
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)


class MarsEmrCreateJobFlowOperator(EmrCreateJobFlowOperator):
    def execute(self, *args, **kwargs):
        # Check if the cluster id is already exist.
        try:
            cluster_key = Variable.get("cluster_key")
        except KeyError:
            cluster_key = None
        self.log.info("Cluster already : %s", cluster_key)

        if cluster_key:
            self.log.info("Cluster already running cluster_key: %s", cluster_key)
            return 0
        cluster_key = super(MarsEmrCreateJobFlowOperator, self).execute(*args, **kwargs)

        # Set the cluster id as variable.
        self.log.info("Created new cluster cluster_key : %s", cluster_key)
        Variable.set("cluster_key", cluster_key)
        return cluster_key
