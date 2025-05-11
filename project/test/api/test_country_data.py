# tests/test_country_data.py

import pytest
import requests
import pandas as pd

from api.data.country_data import (
    fetch_country_data,
    process_country_data,
    create_country_dataframe,
    filter_countries_with_language,
    process_country_data_workflow
)

class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"HTTP {self.status_code}")

def test_fetch_country_data_success(monkeypatch):
    sample_json = {"CountryResource": {"Countries": {"Country": []}}}
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(sample_json, status_code=200)
    )
    result = fetch_country_data("http://fake", headers={})
    assert result == sample_json

def test_fetch_country_data_http_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse({}, status_code=500)
    )
    result = fetch_country_data("http://fake", headers={})
    assert result is None
    captured = capsys.readouterr()
    assert "Error fetching data" in captured.out

def test_process_country_data_multiple_names():
    country_data = {
        "CountryResource": {
            "Countries": {
                "Country": [
                    {
                        "CountryCode": "AA",
                        "Names": {
                            "Name": [
                                {"@LanguageCode": "EN", "$": "Country EN"},
                                {"@LanguageCode": "FR", "$": "Pays FR"}
                            ]
                        }
                    }
                ]
            }
        }
    }
    result = process_country_data(country_data)
    assert isinstance(result, list)
    assert any(r["LanguageCode"] == "EN" and r["CountryName"] == "Country EN" for r in result)
    assert any(r["LanguageCode"] == "FR" and r["CountryName"] == "Pays FR" for r in result)

def test_process_country_data_single_name():
    country_data = {
        "CountryResource": {
            "Countries": {
                "Country": [
                    {
                        "CountryCode": "BB",
                        "Names": {"Name": {"@LanguageCode": "EN", "$": "OnlyName"}}
                    }
                ]
            }
        }
    }
    result = process_country_data(country_data)
    assert result == [{"CountryCode": "BB", "LanguageCode": "EN", "CountryName": "OnlyName"}]

def test_create_country_dataframe():
    data = [
        {"CountryCode": "X1", "LanguageCode": "EN", "CountryName": "X One"},
        {"CountryCode": "Y2", "LanguageCode": "FR", "CountryName": "Y Deux"},
    ]
    df = create_country_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["CountryCode", "LanguageCode", "CountryName"]
    assert df.shape == (2, 3)

def test_filter_countries_with_language():
    df = pd.DataFrame([
        {"CountryCode": "C1", "LanguageCode": "EN"},
        {"CountryCode": "C1", "LanguageCode": "FR"},
        {"CountryCode": "C2", "LanguageCode": "FR"}
    ])
    result = filter_countries_with_language(df, "EN")
    assert result == ["C1"]

def test_process_country_data_workflow(monkeypatch):
    fake_info = [{"CountryCode": "ZZ", "LanguageCode": "EN", "CountryName": "Zed"}]
    monkeypatch.setattr(
        "api.data.country_data.fetch_country_data",
        lambda url, headers: {"dummy": True}
    )
    monkeypatch.setattr(
        "api.data.country_data.process_country_data",
        lambda data: fake_info
    )
    df = process_country_data_workflow({}, ["u1", "u2"])
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert all(df["CountryCode"] == "ZZ")
    assert all(df["CountryName"] == "Zed")
