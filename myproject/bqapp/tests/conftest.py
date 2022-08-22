"""conftest
"""
import os
from unittest import mock
from datetime import datetime

import pytest
from google.cloud import bigquery
from django.utils.timezone import make_aware
# from google.api_core import exceptions


def mocked_query_job(job_id: str) -> bigquery.QueryJob:
    """モックしたQueryJob

    job_id: "jobid_running", "jobid_done", "jobid_fail" のいずれか
    """
    qj = mock.Mock()
    error_result = None
    if job_id == "jobid_running":
        running_ret, state, result_ret = True, "RUNNING", None
    elif job_id == "jobid_done":
        running_ret, state, result_ret = False, "DONE", [('a', 1), ('b', 2)]
    elif job_id == "jobid_failure":
        running_ret, state, result_ret = False, "DONE", None
        error_result = 'failure'
    else:
        running_ret, state, result_ret = False, "PENDING", None
    qj.job_id = job_id
    qj.running.return_value = running_ret
    qj.state = state
    qj.started = make_aware(datetime(2022, 8, 22, 10, 10, 10))
    qj.error_result = error_result
    qj.result.return_value = result_ret
    return qj


@pytest.fixture
def mocked_query_job_running():
    return mocked_query_job("jobid_running")


@pytest.fixture
def mocked_query_job_done():
    return mocked_query_job("jobid_done")


# @pytest.fixture
# def mocked_query_job_done_run_3_times():
#     qj = mock.Mock()
#     qj.job_id = "jobid_done_run_3_times"
#     qj.running.side_effect = [True, True, True, False]
#     qj.result.return_value = [('a', 1), ('b', 2)]
#     qj.state = "DONE"
#     return qj


@pytest.fixture
def create_qj():
    """QueryJobを作成する
    """
    project = os.getenv("GOOGLE_CLOUD_PROJECT", None)
    location = os.getenv("GOOGLE_CLOUD_LOCATION", None)
    client = bigquery.Client(project=project, location=location)
    query_str = """SELECT
  CONCAT(
    'https://stackoverflow.com/questions/',
    CAST(id as STRING)) as url,
  view_count
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE tags like '%google-bigquery%'
ORDER BY view_count DESC
LIMIT 10;
    """
    query_job = client.query(query_str)

    yield query_job

    job_id = query_job.job_id
    location = query_job.location
    # print(f"job_id: {job_id}")
    _ = client.cancel_job(job_id, location=location)
    # print(f"Job {_.location}:{_.job_id} was cancelled.")

    client.delete_job_metadata(job_id, location=location)
    # try:
    #     client.get_job(job_id, location=location)
    # except exceptions.NotFound:
    #     print(f"Job metadata for job {location}:{job_id} was deleted.")
