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
