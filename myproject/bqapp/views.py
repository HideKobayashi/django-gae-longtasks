"""Views
"""
# from django.shortcuts import render
from django.views.generic import TemplateView, ListView
from django.http import JsonResponse

from bqapp.models import MainTask


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
