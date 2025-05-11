'''
Author: Jackyiii feiyi0727@gmail.com
Date: 2025-05-11 16:24:04
LastEditors: Jackyiii feiyi0727@gmail.com
LastEditTime: 2025-05-11 16:24:22
FilePath: /project/test/test_language_code.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
# tests/test_language_codes.py

import pytest
import requests
import pandas as pd
import runpy

from io import StringIO

class DummyResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

def test_language_codes_processing(monkeypatch, capsys, tmp_path):
    sample_csv = """alpha3-b,alpha3-t,alpha2,English,French
eng,eng,en,English,Anglais
fra,fre,fr,French,Français
foo,foo,zz,ZedZed,ZedFrancais
"""
    monkeypatch.setattr(requests, "get", lambda url: DummyResponse(sample_csv, 200))

    # execute the script and capture its globals
    globs = runpy.run_path("language_codes.py")

    df = globs["languages_df"]
    # check columns
    assert list(df.columns) == ["LanguageCode", "LanguageName", "Region"]
    # check that codes are uppercase
    assert set(df["LanguageCode"]) == {"EN", "FR", "ZZ"}
    # check LanguageName
    assert "English" in df["LanguageName"].values
    assert "Français" in df["LanguageName"].values
    # check region mapping for known codes
    assert df.loc[df["LanguageCode"] == "EN", "Region"].iat[0] == "Monde entier"
    assert df.loc[df["LanguageCode"] == "FR", "Region"].iat[0] == "Monde entier"
    # script prints head and shape
    out = capsys.readouterr().out
    assert "LanguageCode" in out
    assert str(df.shape) in out

def test_language_codes_http_error(monkeypatch, capsys):
    monkeypatch.setattr(requests, "get", lambda url: DummyResponse("", 404))
    globs = runpy.run_path("language_codes.py")
    out = capsys.readouterr().out
    assert "Erreur lors du téléchargement du fichier CSV" in out
    assert "languages_df" not in globs
