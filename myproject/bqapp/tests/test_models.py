"""test models
"""
from datetime import datetime
import pytest
# from pytest import param as pp

from bqapp.models import MainTask, SubTask

from django.utils.timezone import make_aware


@pytest.fixture
def db_mt():
    """MainTask にテスト用レコードを入れる
    """
    model = MainTask
    columns = (
        "main_job_id",
        "main_task_name",
        "process_state",
        "process_start_time",
        "in_data",
        "out_data",
    )
    rows = [
        ("jobId9001", "main_task1", "未処理",
            make_aware(datetime(2022, 8, 20, 10, 10, 10)), "NO_001", "aaa"),
        ("jobId9002", "main_task1", "未処理",
            make_aware(datetime(2022, 8, 20, 10, 10, 10)), "NO_002", "aaa"),
    ]
    record_list = []
    for row in rows:
        args = dict(zip(columns, row))
        new_record = model(**args)
        new_record.save()
        record_list.append(new_record)

    yield record_list

    model.objects.all().delete()


@pytest.fixture
def db_st(db_mt):
    """SubTask にテスト用レコードを入れる

    MainTask にテスト用レコードが入っている前提で、MainTask の先頭のレコードを
    SubTask の外部キーとする。
    """
    mt_record_list = db_mt
    new_mt_record = mt_record_list[0]

    model = SubTask
    columns = (
        "sub_job_id",
        "sub_task_name",
        "process_state",
        "process_start_time",
        "in_data",
        "out_data",
        "main_task",
    )
    rows = [
        ("jobId0011", "sub_task1", "未処理", make_aware(datetime(2022, 8, 20, 10, 10, 10)),
            '{"target_catalog": "NO_001"}', '{"table_id", "aaa"}', new_mt_record),
    ]
    record_list = []
    for row in rows:
        args = dict(zip(columns, row))
        new_record = model(**args)
        new_record.save()
        record_list.append(new_record)

    yield record_list

    model.objects.all().delete()


@pytest.mark.django_db
class TestMainTask:
    """単体テスト
    """
    def test_main_task_ok(self, db_mt):
        """正常系
        """
        record_list = db_mt
        expect = len(record_list)

        records = MainTask.objects.all()
        actual = len(records)

        print(f"actual: {actual}", end="| ")
        assert expect == actual

    def test_sub_task_ok(self, db_st):
        """正常系
        """
        record_list = db_st
        expect = len(record_list)

        records = SubTask.objects.all()
        actual = len(records)

        print(f"actual: {actual}", end="| ")
        assert expect == actual
