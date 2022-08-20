"""BigQuery Requests
"""
from google.cloud import bigquery
import time

project = "bigquery-poc-220606"
location = None
# location = "asia-notheast1"


def query_sleep(sleep_time: int = 5):
    interval = 1
    client = bigquery.Client(project=project, location=location)
    query_job = client.query(
        f"CALL `bigquery-poc-220606.tools_tokyo.sleep`({sleep_time})"
    )

    job_id = query_job.job_id

    while query_job.running():
        print(f"job_id: {job_id}, Job state: {query_job.state}")
        time.sleep(interval)

    print(f"job_id: {job_id}, Job state: {query_job.state}")


class BqScript:
    """BigQuery Script
    """
    def __init__(self):
        """
        """
    
    def s_task1_begin(self):
        """最初のタスク
        """
        return query_sleep()

    def s_task2_mid(self):
        """２番目のタスク
        """
        return query_sleep()

    def s_task3_mid(self):
        """3番目のタスク
        """
        return query_sleep()

    def s_task4_mid(self):
        """4番目のタスク
        """
        return query_sleep()

    def s_task5_end(self):
        """5番目のタスク
        """
        return query_sleep()
