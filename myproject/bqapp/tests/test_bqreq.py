"""Test bqapp
"""
from unittest import mock
import pytest
from pytest import param as pp

from google.cloud import bigquery

from bqapp.bqreq import BqScript


@pytest.mark.skip
class TestBqScript_NoMock:
    """BqScript の単体テスト
    """
    def test_s_tesk1(self):
        """ メソッド s_task1
        """
        bqscript = BqScript()

        actual = bqscript.s_task1_begin()

        print(f"actual: {actual}", end="| ")


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


class TestBqScript:
    """BqScript の単体テスト
    """
    def test_s_tesk1(self):
        """ メソッド s_task1
        """
        main_job_id = "main_jobid"
        main_task_name = "main_task"
        job_id = "jobid_done"
        expect_job_id = job_id
        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().query.return_value = mocked_query_job(job_id)
            bqscript = BqScript(main_job_id, main_task_name)
            actual = bqscript.s_task1_begin()

        print(f"actual: {actual}", end="| ")
        assert actual["job_id"] == expect_job_id

    @pytest.mark.parametrize('task_name, expect_message', [
        pp("task1_begin", "Next job is started", id="task1_begin"),
        pp("task2_mid", "Next job is started", id="task2_mid"),
        pp("task3_mid", "Next job is started", id="task3_mid"),
        pp("task4_mid", "Next job is started", id="task4_mid"),
        pp("task5_end", "Final process state is", id="task5_end"),
    ])
    def test_urge_process_to_go_forward(self, task_name, expect_message):
        """ メソッド urge_process_to_go_forward
        """
        main_job_id = "main_jobid"
        main_task_name = "main_task"
        job_id = "jobid_done"
        next_job_id = "jobid_running"

        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(next_job_id)
            bqscript = BqScript(main_job_id, main_task_name)
            job_info, message = bqscript.urge_process_to_go_forward(task_name, job_id)
        actual = message

        print(f"actual: {actual}", end="| ")
        assert actual.startswith(expect_message)
