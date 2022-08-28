"""Test Views
"""
from unittest import mock
# import pytest
# from pytest import param as pp

# from google.cloud import bigquery

# from bqapp.bqreq import BqScript, get_job_state
from bqapp.views import do_update_task_db_tables
from bqapp.tests.conftest import mocked_query_job


class TestDoUpdateTaskDbTables:
    """処理状態を更新する
    """
    # def test_ok(self, db_st):
    #     record = 
    #     sub_task_dict = {}
    #     next_job_id = "jobid_running"
    #     next_task_data = {}

    #     # sub_task_state = "DONE"
    #     # next_job_id = "jobid_done"
    #     # next_task_data = {}

    #     # sub_task_state = "FAIL"
    #     # next_job_id = "jobid_fail"
    #     # next_task_data = {}

    #     with mock.patch('bqapp.bqreq.bigquery.Client', autospec=True) as mock_client:
    #         mock_client().get_job.return_value = mocked_query_job(job_id)
    #         # mock_client().query.return_value = mocked_query_job(next_job_id)
    #         actual = do_update_task_db_tables(record, sub_task_dict, next_task_dict)

    #     print(f"actual: {actual}", end="| ")
