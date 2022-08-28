"""BigQuery Requests
"""
from typing import Tuple, Union, List
import os
import time
import json
from datetime import datetime
from typing import Optional

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
    """query fake
    """
    rdict = {"job_id": None, "state": None, "query_job": None}
    # rdict["project"] = project
    # rdict["location"] = location
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
    rdict["query_job"] = query_job
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


def get_job_state(job_id: str) -> Tuple[str, Optional[str]]:
    """QueryJob の state を取得する
    """
    # rdict = {"job_id": None, "state": None}
    # rdict["project"] = project
    # rdict["location"] = location
    client = bigquery.Client(project=project, location=location)
    query_job = client.get_job(job_id, project=project, location=location)
    return query_job.state, query_job.error_result


def judge_job_state(state: str, error_result: Optional[str]) -> str:
    """QueryJobの state と error_result から状態を判断する
    """
    if state == 'RUNNING':
        judge = 'RUNNING'
    elif state == 'DONE':
        if error_result:
            judge = 'DONE-FAILURE'
        else:
            judge = 'DONE-SUCCESS'
    else:
        judge = 'PENDING'
    return judge


def get_process_state(job_id_list: List[str]) -> str:
    """ジョブIDリストの複数のプロセス状態を取得して判定結果を返す

    DONE-SUCCESS: 全てのジョブが完了
    DONE-FAILURE: 上記以外で、一つでも失敗したジョブがある
    PENDING: 上記以外で、一つでも保留にされたジョブがある
    RUNNING: 上記以外で、一つでも処理中のジョブがある
    """
    judge = ""
    judge_list = []
    for job_id in job_id_list:
        state, error_result = get_job_state(job_id)
        single_judge = judge_job_state(state, error_result)
        single_judge_dict = {'job_id': job_id, 'judge': single_judge}
        judge_list.append(single_judge_dict)
    # judge_bool_list = [x['judge'] == 'DONE-SUCCESS' for x in judge_list]
    # print(f"judge_list: {judge_list}")
    # print(f"judge_bool_list: {judge_bool_list}")
    # print(f"all(judge_bool_list): {all(judge_bool_list)}")
    if all([x['judge'] == 'DONE-SUCCESS' for x in judge_list]):
        judge = 'DONE-SUCCESS'
    elif any([x['judge'] == 'DONE-FAILURE' for x in judge_list]):
        judge = 'DONE-FAILURE'
    elif any([x['judge'] == 'PENDING' for x in judge_list]):
        judge = 'PENDING'
    elif any([x['judge'] == 'RUNNING' for x in judge_list]):
        judge = 'RUNNING'
    return judge


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
        next_job_dict = {}
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
        state, error_result = get_job_state(job_id)
        state = judge_job_state(state, error_result)

        if state == "RUNNING":
            message = f"Current process state is {state}. (job_id: {job_id}, task_name: {task_name}) "
        # ジョブが終わっていたら次のジョブをスタートする
        elif state == "DONE-SUCCESS":
            if next_task_func is None:
                # 一連の全ての子タスクが終了したことを知らせる
                message = f"Final process state is {state}. (job_id: {job_id}, task_name: {task_name})"
            else:
                # 次のタスクを実行して次のタスクの job_id を得る
                job_id, rdict = next_task_func()
                next_job_id = rdict.get("job_id", None)
                subtask_columns = (
                    'sub_job_id', 'sub_task_name', 'process_state', 'process_start_time', 'in_data', 'out_data')        
                next_job_dict = {col: None for col in subtask_columns}
                next_job_dict['sub_job_id'] = rdict.get("job_id")
                next_job_dict['sub_task_name'] = next_task_name
                next_job_dict['process_state'] = state
                next_job_dict['process_start_time'] = rdict['query_job'].started
                next_job_dict['in_data'] = json.dumps({})
                next_job_dict['out_data'] = json.dumps({})
                message = f"Next job is started. (next_job_id: {next_job_id}, next_task_name: {next_task_name}"
        elif state == "DONE-FAILURE":
            message = f"Last process state is {state}. (job_id: {job_id}, task_name: {task_name})"
        else:
            message = f"Current process state is {state}. (job_id: {job_id}, task_name: {task_name})"

        sub_task_dict = {"state": state, "job_id": job_id, "task_name": task_name}
        # スタートした次のジョブの job_id を返す
        # return next_job_id, message
        return next_job_dict, message, sub_task_dict

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
