from django.urls import path
from bqapp.views import JobMngView, MainTaskView

urlpatterns = [
    path('jobmng', JobMngView.as_view(), name="jobmng"),
    path('maintask', MainTaskView.as_view(), name="maintask_list"),
]
