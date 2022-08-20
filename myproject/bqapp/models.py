"""models
"""
from django.db import models


PROCESS_STATE_CHOICES = (
    ("0", "未処理"),
    ("1", "処理済"),
    ("2", "処理中"),
    ("3", "失敗"),
)


class MainTask(models.Model):
    """メインタスク
    """
    main_job_id = models.CharField("メインジョブID", max_length=255, unique=True)
    main_task_name = models.CharField("メインタスク名", max_length=255)
    process_state = models.CharField("処理状態", max_length=255, choices=PROCESS_STATE_CHOICES)
    process_start_time = models.DateTimeField("処理開始時刻")
    in_data = models.JSONField("入力データ")
    out_data = models.JSONField("出力データ")
    created_at = models.DateTimeField("登録日時", auto_now_add=True)
    updated_at = models.DateTimeField("更新日時", auto_now=True)

    class Meta:
        db_table = "main_task"


class SubTask(models.Model):
    """サブタスク
    """
    sub_job_id = models.CharField("ジョブID", max_length=255, unique=True)
    sub_task_name = models.CharField("サブタスク名", max_length=255)
    process_state = models.CharField("処理状態", max_length=255, choices=PROCESS_STATE_CHOICES)
    process_start_time = models.DateTimeField("処理開始時刻")
    in_data = models.JSONField("入力データ")
    out_data = models.JSONField("出力データ")
    main_task = models.ForeignKey(MainTask, verbose_name="", on_delete=models.CASCADE)
    created_at = models.DateTimeField("登録日時", auto_now_add=True)
    updated_at = models.DateTimeField("更新日時", auto_now=True)

    class Meta:
        db_table = "sub_task"
