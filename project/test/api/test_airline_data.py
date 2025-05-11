# tests/test_airline_data.py

import pytest
import requests
import pandas as pd

from api.data.airline_data import (
    fetch_airline_data,
    create_airline_dataframe,
    process_airline_data_workflow
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

def test_fetch_airline_data_success(monkeypatch):
    sample_json = {
        "AirlineResource": {
            "Airlines": {
                "Airline": [
                    {
                        "AirlineID": "123",
                        "AirlineID_ICAO": "ABC",
                        "Names": {
                            "Name": [
                                {"@LanguageCode": "EN", "$": "TestAir EN"},
                                {"@LanguageCode": "FR", "$": "TestAir FR"}
                            ]
                        }
                    },
                    {
                        "AirlineID": "456",
                        "AirlineID_ICAO": "DEF",
                        "Names": {}
                    }
                ]
            }
        }
    }

    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(sample_json, status_code=200)
    )

    result = fetch_airline_data("http://fake.url", headers={"X": "Y"})
    assert isinstance(result, list)
    assert any(r["LanguageCode"] == "EN" and r["AirlineName"] == "TestAir EN" for r in result)
    assert any(r["LanguageCode"] == "FR" and r["AirlineName"] == "TestAir FR" for r in result)
    assert any(r["AirlineID"] == "456" and r["AirlineName"] is None for r in result)

def test_fetch_airline_data_http_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse({}, status_code=500)
    )

    res = fetch_airline_data("http://fake.url", headers={})
    assert res == []
    captured = capsys.readouterr()
    assert "Error fetching data" in captured.out

def test_create_airline_dataframe():
    data = [
        {"AirlineID": "X1", "AirlineID_ICAO": "XIC", "LanguageCode": "EN", "AirlineName": "X One"},
        {"AirlineID": "Y2", "AirlineID_ICAO": "YIC", "LanguageCode": "FR", "AirlineName": "Y Deux"},
    ]

    df = create_airline_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["AirlineID", "AirlineID_ICAO", "LanguageCode", "AirlineName"]
    assert df.shape == (2, 4)

def test_process_airline_data_workflow(monkeypatch):
    fake = [
        {"AirlineID": "Z1", "AirlineID_ICAO": "ZIC", "LanguageCode": "EN", "AirlineName": "Zed One"}
    ]
    monkeypatch.setattr(
        "api.data.airline_data.fetch_airline_data",
        lambda url, headers: fake
    )

    headers = {"Auth": "token"}
    urls = ["url1", "url2", "url3"]
    df = process_airline_data_workflow(headers, urls)

    assert df.shape[0] == 3
    assert (df["AirlineID"] == "Z1").all()
