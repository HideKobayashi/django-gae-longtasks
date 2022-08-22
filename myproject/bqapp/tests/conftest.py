"""conftest
"""
import os
from unittest import mock

import pytest
from google.cloud import bigquery


def mocked_query_job(job_id: str) -> bigquery.QueryJob:
    """モックしたQueryJob

    job_id: "jobid_running", "jobid_done", "jobid_fail" のいずれか
    """
    qj = mock.Mock()
    if job_id == "jobid_running":
        running_ret, state, result_ret = True, "RUNNING", None
    elif job_id == "jobid_done":
        running_ret, state, result_ret = False, "DONE", [('a', 1), ('b', 2)]
    elif job_id == "jobid_fail":
        running_ret, state, result_ret = False, "FAIL", None
    else:
        running_ret, state, result_ret = False, "UNKNOWN", None
    qj.job_id = job_id
    qj.running.return_value = running_ret
    qj.state = state
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
    sleep_time = 5
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
    query_str += f"""DECLARE DELAY_TIME DATETIME;
DECLARE WAIT BOOL;

BEGIN
  SET WAIT = TRUE;
  SET DELAY_TIME = DATE_ADD(CURRENT_DATETIME, INTERVAL {sleep_time} SECOND);
  WHILE WAIT DO
    IF (DELAY_TIME < CURRENT_DATETIME) THEN
      SET WAIT = FALSE;
    END IF;
  END WHILE;
END
    """
    query_job = client.query(query_str)

    yield query_job

    job_id = query_job.job_id
    print(f"job_id: {job_id}")
    canceled_job = client.cancel_job(job_id)
    print(f"{canceled_job.location}:{canceled_job.job_id} cancelled")
