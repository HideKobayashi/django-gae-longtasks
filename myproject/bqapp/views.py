"""Views
"""
from django.shortcuts import render
from django.views.generic import TemplateView, ListView
from django.http import JsonResponse

from bqapp.models import MainTask, SubTask
from bqapp.bqreq import BqScript
# from bqapp.bqreq import get_job_state


class JobMngView(TemplateView):
    """ジョブ管理のための View
    """
    def get(self, request, *args, **kwargs):
        """GET リクエストを受け取ったときに呼ばれるメソッド
        """
        response = {}
        response["message"] = "Hello"
        return JsonResponse(response)


class MainTaskView(ListView):
    """MainTaskView を表示する
    """
    template_name = "bqapp/maintask_list.html"
    model = MainTask
    context_object_name = "object_list"
    object_list = MainTask.objects.all().order_by("-created_at")

    def get(self, request, *args, **kwargs):
        """GET リクエストを受け取ったときに呼ばれるメソッド
        """
        # パラメータチェック
        start_bq_script = request.GET.get("startBqScript")
        print(f"args: {args}, kwargs: {kwargs}")

        # 画面表示のためのcontextを取得
        context = super().get_context_data(**kwargs)
        context['object_list'] = self.object_list
        print(f"context: {context}")

        # メインタスクタスクを開始してDB登録
        if start_bq_script:
            # メインタスクのBigQueryスクリプトを開始する
            bqscript = BqScript(main_task_name="BqScript1")
            rdict, sub_rdict = bqscript.start_main_task()

            # MainTaskにレコードを登録する
            record = MainTask(**rdict)
            record.save()

            # SubTaskにレコードを登録する
            sub_rdict["main_task"] = record
            st_record = SubTask(**sub_rdict)
            st_record.save()

        return render(request, self.template_name, context)


class SubTaskView(ListView):
    """SubTaskView を表示する
    """
    template_name = "bqapp/subtask_list.html"
    model = SubTask
    context_object_name = "object_list"
    object_list = SubTask.objects.all().order_by("-created_at")

    def get(self, request, *args, **kwargs):
        """GET リクエストを受け取ったときに呼ばれるメソッド
        """
        # パラメータチェック
        update_status = request.GET.get("updateStatus")
        # print(f"args: {args}, kwargs: {kwargs}")

        # 画面表示のためのcontextを取得
        context = super().get_context_data(**kwargs)
        context['object_list'] = self.object_list
        # print(f"context: {context}")

        # DB更新
        if update_status:
            # SubTaskのレコードで"RUNNING"のレコードリストを取得する
            st_records = SubTask.objects.filter(process_state="RUNNING")
            # レコードリストについて繰り返す
            for record in st_records:
                # sub_job_id を取得
                sub_job_id = record.sub_job_id
                sub_task_name = record.sub_task_name
                main_job_id = record.main_task.main_job_id
                main_task_name = record.main_task.main_task_name

                # sub_job_id の処理状態を取得しサブタスクの実行を進める
                bqscript = BqScript(main_job_id, main_task_name)
                next_job_dict, message, sub_task_dict = bqscript.urge_process_to_go_forward(
                    sub_task_name, sub_job_id)
                next_job_dict['main_task'] = record.main_task

                # ＜サブタスクとメインタスクの処理状態を更新する＞
                # update_task_db_tables(record, sub_task_dict,  next_task_dict)

        return render(request, self.template_name, context)


def do_update_task_db_tables(
        record: SubTask,
        sub_task_dict: dict,
        next_task_dict: dict):
    """サブタスクとメインタスクの処理状態を更新する
    """
    # サブタスクのパラメータ取得
    sub_task_state = sub_task_dict["state"]
    # sub_job_id = sub_task_dict["job_id"]
    # sub_task_name = sub_task_dict["task_name"]
    # サブタスクの処理状態が RUNNING なら
    if sub_task_state == 'RUNNNING':
        # サブタスクの処理状態を　RUNNNINGで更新
        record.process_state = 'RUNNING'
        # メインタスクの処理状態を RUNNING で更新
        record.main_task.process_state = 'RUNNING'
        branch = 'running'
    # サブタスクの処理状態が　DONE-SUCCESS 
    elif sub_task_state == 'DONE-SUCCESS':
        # かつ 次のタスクがあるなら
        if next_task_dict:
            # サブタスクの処理状態を DONE で更新
            record.process_state = 'DONE-SUCCESS'
            # サブタスクに次のタスクを処理状態 RUNNING で登録
            next_st_record = SubTask(**next_task_dict)
            next_st_record.save()
            # 登録するフィールド:
            #   sub_job_id, sub_task_name, process_state,
            #   process_start_time, in_data, out_data, main_task
            # メインタスクの処理状態を RUNNING で更新
            # メインタスクの処理状態を RUNNING で更新
            record.main_task.process_state = 'RUNNING'
            branch = 'done-middle'
        # かつ 次のサブタスクがないなら
        else:
            # サブタスクの処理状態を　DONE で更新
            record.process_start_time = 'DONE-SUCCESS'
            # メインタスクの処理状態を DONE で更新
            record.main_task.process_state = 'DONE-SUCCESS'
            branch = 'done-final'
    # サブタスクの処理状態が　FAIL なら
    elif sub_task_state == 'DONE-FAILURE':
        # サブタスクの処理状態を FAIL で更新
        record.process_state = 'DONE-FAILURE'
        # メインタスクの処理状態を FAIL で更新
        record.main_task.process_state = 'DONE-FAILURE'
        branch = 'done-failure'
    else:
        # サブタスクの処理状態を PENDING で更新
        record.process_state = 'PENDING'
        # メインタスクの処理状態を FAIL で更新
        record.main_task.process_state = 'PENDING'
        branch = 'pending'
    record.save()
    record.main_task.save()
    return branch
