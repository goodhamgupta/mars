from typing import Dict
import boto3, json, pprint, requests, textwrap, time, logging
from airflow.models import Variable
from datetime import datetime


class EmrClient:
    """
    Helper class to access EMR
    """

    def __init__(self):
        self.client = boto3.client(
            "emr",
            region_name="ap-south-1",
            aws_access_key_id=Variable.get("aws_access_key_id"),
            aws_secret_access_key=Variable.get("aws_secret_access_key"),
        )

    def get_cluster_dns(self, cluster_key: str) -> str:
        """
        Function to get the Master server DNS given the cluster_key

        :param cluster_key: Key to identify EMR cluster

        :return: Master server DNS
        :rtype str
        """
        response = self.client.describe_cluster(ClusterId=cluster_key)
        logging.info(response)
        return response["Cluster"]["MasterPublicDnsName"]

    def wait_for_cluster_creation(self, cluster_key: str):
        """
        Wait till EMR cluster is in "Ready" state. This is required because the public DNS for the cluster will be created only once the cluster is ready.
        """
        self.client.get_waiter("cluster_running").wait(ClusterId=cluster_key)

    def create_spark_session(self, kind: str="sql") -> Dict[str,str]:
        """
        Creates an interactive scala spark session.

        :param kind: Type of spark session. It can be one of the following:
        - pyspark => Python
        - sparkr => R
        - sql => SQL
        :type str

        :return: Dict containing response headers
        :rtype dict
        """
        cluster_dns = Variable.get("cluster_dns")
        host = f"http://{cluster_dns}:8998"  # 8998: Apache Livy server
        data = {"kind": kind}
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{host}/sessions", data=json.dumps(data), headers=headers
        )
        logging.info(response.json())
        return response.headers

    def wait_for_idle_session(self, master_dns: str, response_headers: Dict[str, str]) -> str:
        """
        Wait for the session to be idle or ready for job submission

        :param master_dns: Public URL for the EMR cluster
        :type str
        :param response_headers: Response headers for the create spark session request
        :type dict

        :return: Session URL
        :rtype str
        """
        status = ""
        host = "http://" + master_dns + ":8998"
        session_url = host + response_headers["location"]
        while status != "idle":
            time.sleep(3)
            status_response = requests.get(session_url, headers=response_headers)
            status = status_response.json()["state"]
            logging.info("Session status: " + status)
        return session_url

    def kill_spark_session(self, session_url: str) -> Dict[str, str]:
        """
        Function to kill the spark session

        :param session_url: URL for spark session
        :type str

        :return: Delete response
        :rtype dict
        """
        requests.delete(session_url, headers={"Content-Type": "application/json"})

    def submit_statement(self, session_url: str, statement_path: str) -> Dict[str, str]:
        """
        Submits the spark code as a simple JSON command to the Livy server

        :param session_url: URL for the spark session
        :type str

        :param statement_path: Path of the file containing the spark code
        :type str

        :return: Response object
        :rtype dict
        """
        statements_url = f"session_url/statements"
        with open(statement_path, "r") as f:
            code = f.read()
        data = {"code": code}
        response = requests.post(
            statements_url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
        )
        logging.info(response.json())
        return response

    def track_statement_progress(self, master_dns, response_headers):
        """
        Function to help track the progress of the scala code submitted to Apache Livy

        :param master_dns: Public URL for the EMR cluster
        :type str
        :param response_headers: Response headers for the create spark session request
        :type dict

        :return: Boolean specifying status of the statement.
        :rtype bool
        """
        statement_status = ""
        host = f"http://{master_dns}:8998"
        session_url = host + response_headers["location"].split("/statements", 1)[0]
        # Poll the status of the submitted scala code
        while statement_status != "available":
            # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
            statement_url = host + response_headers["location"]
            statement_response = requests.get(
                statement_url, headers={"Content-Type": "application/json"}
            )
            statement_status = statement_response.json()["state"]
            logging.info("Statement status: " + statement_status)

            lines = requests.get(
                session_url + "/log", headers={"Content-Type": "application/json"}
            ).json()["log"]
            for line in lines:
                logging.info(line)

            if "progress" in statement_response.json():
                logging.info("Progress: " + str(statement_response.json()["progress"]))
            time.sleep(5)
        final_statement_status = statement_response.json()["output"]["status"]
        if final_statement_status == "error":
            logging.info(
                "Statement exception: " + statement_response.json()["output"]["evalue"]
            )
            for trace in statement_response.json()["output"]["traceback"]:
                logging.info(trace)
            raise ValueError("Final Statement Status: " + final_statement_status)
        logging.info("Final Statement Status: " + final_statement_status)
        return True

    def get_public_ip(self):
        """
        Function to fetch the EMR public address

        :return: Public IP address for EMR cluster
        :rtype str
        """
        instances = emr.list_instances(
            ClusterId=self.cluster_key, InstanceGroupTypes=["MASTER"]
        )
        return instances["Instances"][0]["PublicIpAddress"]
