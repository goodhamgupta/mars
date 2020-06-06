from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
from pyhive import hive


class HiveEmrOperator(BaseOperator):
    """
    Operator to use JDBC for Hive connections
    """

    ui_color = "#ffb84d"

    @apply_defaults
    def __init__(self, params, *args, **kwargs):

        super(HiveEmrOperator, self).__init__(*args, **kwargs)
        self.hive_connection = params.get("hive_operator__connection", "hive_again")
        self.async_flag = params.get("hive_operator__async", True)
        self.query = params.get("hive_operator__query")
        self.return_value = params.get("hive_operator__return_value", False)

    def _create_cursor(self):
        """
        Function to create cursor to hive DB. All queries are executed
        using this cursor.
        """
        airflow_conn = BaseHook.get_connection(self.hive_connection)
        conn = hive.Connection(
            host=airflow_conn.host,
            port=airflow_conn.port,
            username=airflow_conn.login,
            password=airflow_conn.password,
        )
        cursor = conn.cursor()
        return cursor

    def execute(self, *args, **kwargs):
        """
        Function to execute the hive query
        """
        cursor = self._create_cursor()
        if self.async_flag:
            # polling logic
            pass
        else:
            # FIXME: Hack to execute multiple queries. Ideally, execute only one query per variable in the hiveql file.
            queries = self.query.split(";")
            for query in queries:
                cursor.execute(query, async_=self.async_flag)
                for line in cursor.fetch_logs():
                    print(line)
            if self.return_value:
                return cursor.fetchall()


class HiveEmrPlugin(AirflowPlugin):
    name = "hive_emr_plugin"
    operators = [HiveEmrOperator]
