"""Test bqapp
"""
from unittest import mock
import pytest
from pytest import param as pp

from bqapp.bqreq import BqScript, get_job_state
from bqapp.tests.conftest import mocked_query_job


# @pytest.mark.skip
class TestBqScript_NoMock:
    """BqScript の単体テスト
    """
    def test_s_task1(self):
        """ メソッド s_task1
        """
        bqscript = BqScript()

        actual = bqscript.s_task1_begin()

        print(f"actual: {actual}", end="| ")


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
            job_id, actual = bqscript.s_task1_begin()

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

    def test_start_main_task(self):
        """ メソッド start_main_task
        """
        main_task_name = "main_task"
        job_id = "jobid_running"

        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(job_id)
            bqscript = BqScript(main_task_name=main_task_name)
            rdict, sub_rdict = bqscript.start_main_task()
        actual = rdict, sub_rdict

        print(f"actual: {actual}", end="| ")


class TestGetJobState:
    """関数　get_job_state のテスト
    """
    def test_ok(self):
        job_id = "jobid_running"
        job_id = "jobid_done"
        job_id = "jobid_fail"

        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(job_id)
            actual = get_job_state(job_id)

        print(f"actual: {actual}", end="| ")

    def test_ok_no_mock(self):
        # job_id = "c121c14d-5212-49e5-b253-ec73cbb37ec7"
        # job_id = "92cd4ab7-59c4-465f-8bc2-7c79c72acd9e"
        # job_id = "d800cd5e-5255-4518-83c5-abb90b074e6c"
        # job_id = "bquxjob_636425cc_182c03b3ded"
        # job_id = "bc6ca690-8671-4d41-b198-f516f3ef2a84 "
        # job_id = "09386c91-59a4-471c-92b0-df3be56cac38"
        job_id = "24d508b7-f6e9-4a71-a8df-027bbe76d385"

        actual = get_job_state(job_id)

        print(f"actual: {actual}", end="| ")
