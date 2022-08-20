"""Test bqapp
"""
import pytest
from pytest import param as pp

from bqapp.bqreq import BqScript


class TestBqScript_NoMock:
    """BqScript の単体テスト
    """
    def test_s_tesk1(self):
        """ メソッド s_task1
        """
        bqscript = BqScript()

        actual = bqscript.s_task1_begin()

        print(f"actual: {actual}", end="| ")