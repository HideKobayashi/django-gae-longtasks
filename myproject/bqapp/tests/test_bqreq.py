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
    def test_urge_process_to_go_forward_subtask_done(self, task_name, expect_message):
        """ メソッド urge_process_to_go_forward
        正常系
        """
        main_job_id = "main_jobid"
        main_task_name = "main_task"
        job_id = "jobid_done"
        next_job_id = "jobid_running"

        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(next_job_id)
            bqscript = BqScript(main_job_id, main_task_name)
            job_info, message, sub_task_dict = bqscript.urge_process_to_go_forward(task_name, job_id)
        actual = message

        print(f"actual: {actual}", end="| ")
        print(f"job_info: {job_info}")
        assert actual.startswith(expect_message)

    @pytest.mark.parametrize('job_id, task_name, expect_message', [
        pp("jobid_failure", "task2_mid", "Last process state is DONE-FAILURE", id="failure"),
        pp("jobid_running", "task2_mid", "Current process state is RUNNING", id="running"),
        pp("jobid_pending", "task2_mid", "Current process state is PENDING", id="pending"),
    ])
    def test_urge_process_to_go_forward_subtask_ng(self, job_id, task_name, expect_message):
        """ メソッド urge_process_to_go_forward
        異常系
        
        条件: サブタスク失敗
        条件: サブタスク待機
        条件: サブタスク進行中
        """
        main_job_id = "main_jobid"
        main_task_name = "main_task"
        # job_id = "jobid_failure"
        next_job_id = "jobid_running"

        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(next_job_id)
            bqscript = BqScript(main_job_id, main_task_name)
            job_info, message, sub_task_dict = bqscript.urge_process_to_go_forward(task_name, job_id)
        actual = message

        print(f"actual: {actual}", end="| ")
        print(f"job_info: {job_info}")
        # assert actual.startswith(expect_message)

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
    @pytest.mark.parametrize('job_id, expect', [
        pp("jobid_running", ('RUNNING', None), id='running'),
        pp("jobid_done", ('DONE', None), id='done'),
        pp("jobid_failure", ('DONE', 'failure'), id='faillure'),
    ])
    def test_ok(self, job_id, expect):
        """正常系
        """
        with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
            mock_client().get_job.return_value = mocked_query_job(job_id)
            mock_client().query.return_value = mocked_query_job(job_id)
            actual = get_job_state(job_id)

        print(f"actual: {actual}", end="| ")
        assert expect == actual

    def test_ok_no_mock(self, create_qj):
        """正常系　モックなし
        """
        query_job = create_qj
        job_id = query_job.job_id

        actual = get_job_state(job_id)

        print(f"actual: {actual}", end="| ")


class TestCreateQj:
    """create_qj のテスト
    """
    def test_create_qj_no_mock(self, create_qj):
        """正常系 モックなし
        """
        query_job = create_qj

        job_id = query_job.job_id
        location = query_job.location
        project = query_job.project
        job_type = query_job.job_type
        job_created_at = query_job.created
        job_started_at = query_job.started
        job_ended_at = query_job.ended
        error_result = query_job.error_result
        state = query_job.state

        print()
        print(f"job_id: {job_id}")
        print(f"location: {location}")
        print(f"project: {project}")
        print(f"job_type: {job_type}")
        print(f"job_created_at: {job_created_at}")
        print(f"job_started_at: {job_started_at}")
        print(f"job_ended_at: {job_ended_at}")
        print(f"error_result: {error_result}")
        print(f"state: {state}")
