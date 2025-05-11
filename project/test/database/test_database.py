'''
Author: Jackyiii feiyi0727@gmail.com
Date: 2025-05-11 16:28:23
LastEditors: Jackyiii feiyi0727@gmail.com
LastEditTime: 2025-05-11 16:28:27
FilePath: /project/test/database/test_database.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
# tests/test_database.py

import pytest
import psycopg2
import api.data.database as db_mod

class DummyCursor:
    def __init__(self):
        self.queries = []
        self.closed = False

    def execute(self, query):
        self.queries.append(query)

    def fetchone(self):
        return ("PostgreSQL 15.3",)

    def close(self):
        self.closed = True

class DummyConnection:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True

def test_get_connection_success(monkeypatch, capsys):
    dummy = object()
    monkeypatch.setattr(db_mod.psycopg2, "connect", lambda dsn: dummy)
    conn = db_mod.get_connection()
    captured = capsys.readouterr()
    assert conn is dummy
    assert "Connected to PostgreSQL database!" in captured.out

def test_get_connection_failure(monkeypatch, capsys):
    def raise_conn(dsn):
        raise Exception("boom")
    monkeypatch.setattr(db_mod.psycopg2, "connect", raise_conn)
    conn = db_mod.get_connection()
    captured = capsys.readouterr()
    assert conn is None
    assert "Error connecting to database:" in captured.out

def test_test_connection_success(monkeypatch, capsys):
    dummy_conn = DummyConnection()
    monkeypatch.setattr(db_mod, "get_connection", lambda: dummy_conn)
    db_mod.test_connection()
    captured = capsys.readouterr()
    assert "Database version:" in captured.out
    assert dummy_conn.cursor_obj.queries == ["SELECT version();"]
    assert dummy_conn.cursor_obj.closed
    assert dummy_conn.closed

def test_test_connection_no_connection(monkeypatch, capsys):
    monkeypatch.setattr(db_mod, "get_connection", lambda: None)
    db_mod.test_connection()
    captured = capsys.readouterr()
    assert captured.out == ""
