"""Test URLs
"""
from django.urls import resolve, reverse
# from django.test import RequestFactory

from bqapp.views import JobMngView


class TestUrls:
    """ URLのテスト
    """
    def test_url_to_view_resolve(self):
        """正常系

        条件: url: "/bqapp/jobmng"
        期待値: ビュー関数のクラスが bqapp.views.JobMngView
        """
        url = "/bqapp/jobmng"
        expect_view_class = JobMngView

        actual = resolve(url)

        print(f"actual: {actual}", end="| ")
        print(f"actual.func: {actual.func}", end="| ")
        print(f"actual.func.view_class: {actual.func.view_class}", end="| ")
        assert expect_view_class == actual.func.view_class 

    def test_viewname_to_url_reverse(self):
        """正常系

        条件: viewname が "jobmng"
        期待値: urlが "/bqapp/jobmng"
        """
        viewname = "jobmng"
        expect_url = "/bqapp/jobmng"

        actual = reverse(viewname)

        print(f"actual: {actual}", end="| ")
        assert expect_url == actual