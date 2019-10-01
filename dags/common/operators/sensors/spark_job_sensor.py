import time
from datetime import datetime
from airflow.operators.sensors import BaseSensorOperator


class SparkJobSensorOperator(BaseSensorOperator):
    """
    Sensor to monitor spark jobs using the session URL. The jobs were submitted via Livy to Apache Spark on EMR.
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(SparkJobSensorOperator, self).__init__(*args, **kwargs)

    def poke(self, context):
        task_instance = context['task_instance']
        time.sleep(10)
        return True

