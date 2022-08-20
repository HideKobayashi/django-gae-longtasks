from django.urls import path
from bqapp.views import JobMngView

urlpatterns = [
    path('jobmng', JobMngView.as_view(), name="jobmng"),
]

