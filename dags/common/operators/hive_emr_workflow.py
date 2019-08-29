from datetime import datetime, timedelta
from random import getrandbits
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import MySqlHook
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from hiveql.myscripbox.interim import offering_source


class HiveEmrWorkflow:
    """
    Class to create subdags for incremetal data imports into hive tables
    """


    def __init__(self, params):
        self.cluster_key = Variable.get('cluster_key')
        self.params = params
        self.source_app = params.get('source_app', 'milkyway')
        self.parent = params.get('parent', 'test')
        self.child = params.get('child', 'test')
        self.snapshot_start = params.get('snapshot_start', datetime.now())
        self.snapshot_end = params.get('snapshot_end', datetime.now())
        self.snapshot_type = params.get('snapshot_type', 'incremental')
        self.destination = params.get('destination')
        self.days = params.get('days', 7)
        self.start_date = params.get('start_date', '2019-01-01')
        self.schedule_interval = params.get('schedule_interval', '0 * * * *')
        self.sql_module = params.get('sql_module', offering_source)
        self.skip_registry = params.get('skip_registry', False)

    @staticmethod
    def _format(datetime_obj):
        """
        Function to format date to the format: YYYY-MM-DD-HH-MM-SS
        """
        return datetime_obj.strftime("%Y-%m-%d")


    def _get_dates(self, snapshot_start, snapshot_end):
        while (snapshot_start <= snapshot_end):
            new = snapshot_start + timedelta(days=self.days)
            if new > snapshot_end:
                yield (self._format(snapshot_start), self._format(snapshot_end))
                break
            else:
                yield(self._format(snapshot_start), self._format(new))
                snapshot_start = new

    def _sql_lookup(self, query, table=None, snapshot_start=None, snapshot_end=None):
        """
        Method to identify and patch the query.
        """
        if query == "CREATE":
            location = Variable.get(f"{self.source_app}_hive_interim_dir")
            return getattr(self.sql_module, query).format(
                external_table=self.destination,
                external_table_location=f'{location}/{self.destination}'
            )
        elif query in ("CREATE_TMP", "COUNT"):
            return getattr(self.sql_module, query)
        elif query == "INSERT":
            return getattr(self.sql_module, query).format(
                external_table=self.destination,
                table=table,
                snapshot_start=snapshot_start,
                snapshot_end=snapshot_end
            )
        elif "REGISTRY" in query:
            table = self.registry_table_name
            return getattr(self.sql_module, query).format(table=table)
        else:
            return getattr(self.sql_module, query).format(
                source_table='events',
                table=table,
                snapshot_start=snapshot_start,
                snapshot_end=snapshot_end
            )


    def _create_steps(self, query):
        arguments = [
            f"{Variable.get('hive_dir')}/hive_cli_expression.sh",
            self._sql_lookup(query)
        ]

        steps = [
            {
                "Name": f"{self.child} {self.snapshot_type} {query}",
                "HadoopJarStep": {
                    "Args": arguments,
                    "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                },
                "ActionOnFailure": "CONTINUE"
            }
        ]

        return steps


    def _snapshot_steps(self, query, table):
        arguments = [
            f"{Variable.get('hive_dir')}/hive_cli_expression.sh",
            self._sql_lookup(query).format(table=table)
        ]

        steps = [
            {
                "Name": f"{self.child} {query} table {table}",
                "HadoopJarStep": {
                    "Args": arguments,
                    "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                },
                "ActionOnFailure": "CONTINUE"
            }
        ]

        return steps

    def _snapshot_generic_steps(self, query, table, snapshot_start, snapshot_end):
        arguments = [
            f"{Variable.get('hive_dir')}/hive_cli_expression.sh",
            self._sql_lookup(query, table, snapshot_start, snapshot_end)
        ]

        steps = [
            {
                "Name": f"{self.child} {self.snapshot_type} {table} {query}: Timerange {snapshot_start} to {snapshot_end}",
                "HadoopJarStep": {
                    "Args": arguments,
                    "Jar": "s3://ap-south-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                },
                "ActionOnFailure": "CONTINUE"
            }
        ]

        return steps


    def _create_external_table(self, dag):
        create_table_steps = self._create_steps("CREATE")

        create_external_table_adder = EmrAddStepsOperator(
            task_id='create_external_table_adder',
            job_flow_id=self.cluster_key,
            aws_conn_id='aws_default',
            steps=create_table_steps,
            dag=dag
        )

        create_external_table_checker = EmrStepSensor(
            task_id='create_external_table_watch',
            job_flow_id=self.cluster_key,
            step_id="{{ task_instance.xcom_pull(task_ids='create_external_table_adder', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            dag=dag
        )

        return (create_external_table_adder, create_external_table_checker)


    def _create_tmp_table(self, dag, table):
        snapshot_create_steps = self._snapshot_steps("CREATE_TMP", table)

        table_adder = EmrAddStepsOperator(
            task_id="table_create",
            job_flow_id=self.cluster_key,
            aws_conn_id='aws_default',
            trigger_rule='none_failed',
            steps=snapshot_create_steps,
            dag=dag
        )
        table_checker = EmrStepSensor(
            task_id='table_watch',
            job_flow_id=self.cluster_key,
            step_id="{{ task_instance.xcom_pull(task_ids='table_create'" + ",key='return_value')[0] }}",
            trigger_rule='none_failed',
            aws_conn_id='aws_default',
            dag=dag
        )

        return (table_adder, table_checker)

    def _create_count(self, dag, table):
        snapshot_count_steps = self._snapshot_steps("COUNT", self.destination)

        count_adder = EmrAddStepsOperator(
            task_id='count',
            job_flow_id=self.cluster_key,
            aws_conn_id='aws_default',
            trigger_rule='none_failed',
            steps=snapshot_count_steps,
            dag=dag
        )

        count_checker = EmrStepSensor(
            task_id='count_watch',
            job_flow_id=self.cluster_key,
            step_id="{{ task_instance.xcom_pull(task_ids='count', key='return_value')[0] }}",
            trigger_rule='none_failed',
            aws_conn_id='aws_default',
            dag=dag
        )

        return (count_adder, count_checker)

    def _full_snapshot(self, params):
        dag = DAG(
            "{}.{}".format(self.parent, self.child),
            schedule_interval=self.schedule_interval,
            start_date=self.start_date
        )

        table = f"{self.destination}_incremental"

        (create_external_table_adder, create_external_table_checker) = self._create_external_table(dag)
        (table_adder, table_checker) = self._create_tmp_table(dag, table)
        (count_adder, count_checker) = self._create_count(dag, table)

        create_external_table_adder >> create_external_table_checker >> table_adder >> table_checker

        gen = self._get_dates(self.snapshot_start, self.snapshot_end)

        with dag:
            for index, (snapshot_start, snapshot_end) in enumerate(gen):
                snapshot_replicate_steps = self._snapshot_generic_steps(
                    "REPLICATE",
                    table,
                    snapshot_start,
                    snapshot_end
                )

                query_adder = EmrAddStepsOperator(
                    task_id=f"query_{index}",
                    job_flow_id=self.cluster_key,
                    aws_conn_id='aws_default',
                    trigger_rule='none_failed',
                    steps=snapshot_replicate_steps,
                    dag=dag
                )
                query_checker = EmrStepSensor(
                    task_id=f'query_watch_{index}',
                    job_flow_id=self.cluster_key,
                    step_id="{{ task_instance.xcom_pull(task_ids=" + f"'query_{index}'" + ",key='return_value')[0] }}",
                    trigger_rule='none_failed',
                    aws_conn_id='aws_default',
                    dag=dag
                )


                snapshot_insert_steps = self._snapshot_generic_steps("INSERT", table, snapshot_start, snapshot_end)

                insert_adder = EmrAddStepsOperator(
                    task_id=f'insert_external_{index}',
                    job_flow_id=self.cluster_key,
                    aws_conn_id='aws_default',
                    trigger_rule='none_failed',
                    steps=snapshot_insert_steps,
                    dag=dag
                )

                insert_checker = EmrStepSensor(
                    task_id=f'insert_external_watch_{index}',
                    job_flow_id=self.cluster_key,
                    step_id="{{ task_instance.xcom_pull(task_ids=" + f"'insert_external_{index}'" + ",key='return_value')[0] }}",
                    trigger_rule='none_failed',
                    aws_conn_id='aws_default',
                    dag=dag
                )

                snapshot_delete_steps = self._snapshot_generic_steps("DELETE_TMP", table, snapshot_start, snapshot_end)

                delete_adder = EmrAddStepsOperator(
                    task_id=f'delete_{index}',
                    job_flow_id=self.cluster_key,
                    aws_conn_id='aws_default',
                    trigger_rule='none_failed',
                    steps=snapshot_delete_steps,
                    dag=dag
                )

                delete_checker = EmrStepSensor(
                    task_id=f'delete_watch_{index}',
                    job_flow_id=self.cluster_key,
                    step_id="{{ task_instance.xcom_pull(task_ids=" + f"'delete_{index}'" + ",key='return_value')[0] }}",
                    trigger_rule='none_failed',
                    aws_conn_id='aws_default',
                    dag=dag
                )

                table_checker >> query_adder >> query_checker
                query_checker >> insert_adder >> insert_checker
                insert_checker >> delete_adder >> delete_checker
                delete_checker >> count_adder >> count_checker

        return dag


    def _incremental_snapshot(self, params):
        pass

    @classmethod
    def create(cls, params):
        """
        Function to create the sub dag to create derived tables in hive. Each sub dag has the following steps:

        - For a selected time range, we first fetch the data using a hive query
        - Store the result in a temporary table(This will be an EMR Step)
        - Write this temporary table to S3(EMR Step)
        - Monitor the status of the writes.(EMR Sensor)
        - Repeat this for all time slices.

        Because the time range is split into slices(usually of size 7 days), longer time ranges will have many EMR steps.
        Make sure the time ranges picked are short or you can afford to wait for a long time for all the steps to complete.
        """
        obj = cls(params)
        if obj.snapshot_type == "full":
            return obj._full_snapshot(params)
        else:
            return obj._incremental_snapshot(params)
