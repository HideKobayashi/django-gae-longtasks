from django.shortcuts import render
from django.views.generic import TemplateView
from django.http import HttpResponse, JsonResponse

# Create your views here.

class JobMngView(TemplateView):
    """ジョブ管理のための View
    """
    def get(self, request, *args, **kwargs):
        """GET リクエストを受け取ったときに呼ばれるメソッド
        """
        response = {}
        response["message"] = "Hello"
        return JsonResponse(response)
