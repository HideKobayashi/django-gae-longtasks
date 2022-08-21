from django.urls import path
from bqapp.views import JobMngView, MainTaskView, SubTaskView

urlpatterns = [
    path('jobmng', JobMngView.as_view(), name="jobmng"),
    path('maintask', MainTaskView.as_view(), name="maintask_list"),
    path('subtask', SubTaskView.as_view(), name="subtask_list"),
]
