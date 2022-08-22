"""BigQuery Requests
"""
import os
import time
import json
from datetime import datetime

from google.cloud import bigquery
from django.utils.timezone import make_aware


class ParamError(Exception):
    """パラメータに不備があるときに上げる例外
    """


project = os.getenv("GOOGLE_CLOUD_PROJECT", None)
location = os.getenv("GOOGLE_CLOUD_LOCATION", None)
# project = "bigquery-poc-220606"
# location = "asia-notheast1"
# location = None


def query_fake(sleep_time: int = 5):
    rdict = {"job_id": None, "state": None}
    rdict["project"] = project
    rdict["location"] = location
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
    # query_str += f"CALL `bigquery-poc-220606.tools_tokyo.sleep`({sleep_time})"
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

    job_id = query_job.job_id

    # while query_job.running():
    #     interval = 1
    #     print(f"job_id: {job_id}, Job state: {query_job.state}")
    #     time.sleep(interval)

    rdict["job_id"] = job_id
    rdict["state"] = query_job.state
    # print(f"job_id: {job_id}, Job state: {query_job.state}")
    return rdict


def query_sleep(sleep_time: int = 5):
    """BigQuery sleep
    """
    client = bigquery.Client(project=project, location=location)
    query_str = ""
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
    job_id = query_job.job_id
    return job_id


def get_job_state(job_id: str):
    """QueryJob の state を取得する
    """
    rdict = {"job_id": None, "state": None}
    rdict["project"] = project
    rdict["location"] = location
    client = bigquery.Client(project=project, location=location)
    query_job = client.get_job(job_id, project=project, location=location)
    return query_job.state


class BqScript:
    """BigQuery Script
    """
    main_job_id = ""
    main_task_name = ""
    progress_dict = {}

    def __init__(self, main_job_id: str = "", main_task_name: str = ""):
        """初期化
        """
        self.main_job_id = main_job_id
        self.main_task_name = main_task_name
        self.tasks_dict = {
            "task1_begin": {"func": self.s_task1_begin, "next": "task2_mid"},
            "task2_mid": {"func": self.s_task2_mid, "next": "task3_mid"},
            "task3_mid": {"func": self.s_task3_mid, "next": "task4_mid"},
            "task4_mid": {"func": self.s_task4_mid, "next": "task5_end"},
            "task5_end": {"func": self.s_task5_end, "next": None},
        }
        return

    def start_main_task(self):
        """メインタスクを開始する

        先頭のタスクを開始して先頭のタスクのjob_id を得る
        """
        rdict = {}
        sub_rdict = {}
        tasks = self.tasks_dict
        head_task_name = "task1_begin"
        head_task_func = tasks[head_task_name]["func"]

        job_id, _out_data = head_task_func()
        now_dt = make_aware(datetime.now())
        self.main_job_id = job_id
        time.sleep(1)
        # state = get_job_state(job_id)
        state = "RUNNING"
        in_data = {}
        out_data = {}

        rdict["main_job_id"] = self.main_job_id
        rdict["main_task_name"] = self.main_task_name
        rdict["process_state"] = state
        rdict["process_start_time"] = now_dt
        rdict["in_data"] = json.dumps(in_data, ensure_ascii=False)
        rdict["out_data"] = json.dumps(out_data, ensure_ascii=False)

        sub_rdict["sub_job_id"] = self.main_job_id
        sub_rdict["sub_task_name"] = head_task_name
        sub_rdict["process_state"] = state
        sub_rdict["process_start_time"] = now_dt
        sub_rdict["in_data"] = json.dumps(in_data, ensure_ascii=False)
        sub_rdict["out_data"] = json.dumps(out_data, ensure_ascii=False)
        sub_rdict["main_task"] = None
        return rdict, sub_rdict

    def urge_process_to_go_forward(self, task_name: str, job_id: str):
        """処理を進めるよう要請する
        """
        next_job_id = None
        tasks = self.tasks_dict
        task = tasks.get(task_name, None)
        if task is None:
            msg = f"No task defined in the tasks_dict: {tasks}"
            raise ParamError(msg)
        # task_func = task["func"]
        next_task_name = task["next"]
        if next_task_name is None:
            next_task_func = None
        else:
            next_task_func = tasks[next_task_name]["func"]

        # job_id のジョブが終わっているか確認する
        # state = get_job_state(job_id)
        state = "DONE"

        if state == "RUNNING":
            message = f"Current task state is {state}. (job_id: {job_id}, task_name: {task_name}) "
        # ジョブが終わっていたら次のジョブをスタートする
        elif state == "DONE":
            if next_task_func is None:
                # 一連の全ての子タスクが終了したことを知らせる
                message = f"Final process state is {state}. (job_id: {job_id}, task_name: {task_name})"
            else:
                # 次のタスクを実行して次のタスクの job_id を得る
                job_id, rdict = next_task_func()
                next_job_id = rdict.get("job_id", None)
                message = f"Next job is started. (next_job_id: {next_job_id}, next_task_name: {next_task_name}"
        else:
            message = f"Current task state is {state}. (job_id: {job_id}, task_name: {task_name})"

        # スタートした次のジョブの job_id を返す
        return next_job_id, message

    def s_task1_begin(self):
        """最初のタスク
        """
        rdict = query_fake()
        job_id = rdict.get("job_id")
        return job_id, rdict

    def s_task2_mid(self):
        """２番目のタスク
        """
        rdict = query_fake()
        job_id = rdict.get("job_id")
        return job_id, rdict

    def s_task3_mid(self):
        """3番目のタスク
        """
        rdict = query_fake()
        job_id = rdict.get("job_id")
        return job_id, rdict

    def s_task4_mid(self):
        """4番目のタスク
        """
        rdict = query_fake()
        job_id = rdict.get("job_id")
        return job_id, rdict

    def s_task5_end(self):
        """5番目のタスク
        """
        rdict = query_fake()
        job_id = rdict.get("job_id")
        return job_id, rdict
