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
                next_job_id, message = bqscript.urge_process_to_go_forward(
                    sub_task_name, sub_job_id)

                # ＜サブタスクとメインタスクの処理状態を更新する＞
                # update_process_status(sub_task_state, next_job_id, next_task_data)

        return render(request, self.template_name, context)


def update_process_status(
        sub_task_state: str,
        next_job_id: str,
        next_task_data: dict):
    """サブタスクとメインタスクの処理状態を更新する
    """
    # サブタスクの処理状態が RUNNING なら
        # サブタスクの処理状態を　RUNNNINGで更新
        # メインタスクの処理状態を RUNNING で更新
    # サブタスクの処理状態が　DONE かつ 次のタスクがあるなら
        # サブタスクの処理状態を DONE で更新
        # サブタスクに次のタスクを処理状態 RUNNING で登録
        # 登録するフィールド:
        #   sub_job_id, sub_task_name, process_state,
        #   process_start_time, in_data, out_data, main_task
        # メインタスクの処理状態を RUNNING で更新
    # サブタスクの処理状態が DONE かつ 次のサブタスクがないなら
        # サブタスクの処理状態を　DONE で更新
        # メインタスクの処理状態を DONE で更新
    # サブタスクの処理状態が　FAIL なら
        # サブタスクの処理状態を FAIL で更新
        # メインタスクの処理状態を FAIL で更新
