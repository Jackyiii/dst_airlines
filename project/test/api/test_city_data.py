# tests/test_city_data.py

import pytest
import requests
import pandas as pd

from api.data.city_data import (
    fetch_city_data,
    process_city_data,
    create_city_dataframe,
    filter_cities_with_language,
    process_city_data_workflow
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

def test_fetch_city_data_success(monkeypatch):
    sample_json = {
        "CityResource": {
            "Cities": {
                "City": [
                    {
                        "CityCode": "C1",
                        "CountryCode": "CO1",
                        "UtcOffset": "+01:00",
                        "TimeZoneId": "TZ1",
                        "Names": {
                            "Name": {"@LanguageCode": "EN", "$": "City EN"}
                        },
                        "Airports": {"AirportCode": "A1"}
                    }
                ]
            }
        }
    }
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(sample_json, status_code=200)
    )
    result = fetch_city_data("http://fake", headers={})
    assert result == sample_json

def test_fetch_city_data_http_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse({}, status_code=500)
    )
    result = fetch_city_data("http://fake", headers={})
    assert result is None
    captured = capsys.readouterr()
    assert "Error fetching data" in captured.out

def test_process_city_data():
    city_data = {
        "CityResource": {
            "Cities": {
                "City": [
                    {
                        "CityCode": "C1",
                        "CountryCode": "CO1",
                        "UtcOffset": "+01:00",
                        "TimeZoneId": "TZ1",
                        "Names": {
                            "Name": [
                                {"@LanguageCode": "EN", "$": "City1 EN"},
                                {"@LanguageCode": "FR", "$": "City1 FR"}
                            ]
                        },
                        "Airports": [
                            {"AirportCode": "A1"},
                            {"AirportCode": "A2"}
                        ]
                    },
                    {
                        "CityCode": "C2",
                        "CountryCode": "CO2",
                        "UtcOffset": "+02:00",
                        "TimeZoneId": "TZ2",
                        "Names": {
                            "Name": {"@LanguageCode": "EN", "$": "City2 EN"}
                        },
                        "Airports": {"AirportCode": "B1"}
                    }
                ]
            }
        }
    }
    result = process_city_data(city_data)
    assert isinstance(result, list)
    entries_c1 = [r for r in result if r["CityCode"] == "C1"]
    assert len(entries_c1) == 4
    entries_c2 = [r for r in result if r["CityCode"] == "C2"]
    assert len(entries_c2) == 1

def test_create_city_dataframe():
    data = [
        {
            "CountryCode": "CO1",
            "CityCode": "C1",
            "LanguageCode": "EN",
            "CityName": "City1",
            "UtcOffset": "+01:00",
            "TimeZoneId": "TZ1",
            "AirportCode": "A1"
        }
    ]
    df = create_city_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "CountryCode",
        "CityCode",
        "LanguageCode",
        "CityName",
        "UtcOffset",
        "TimeZoneId",
        "AirportCode"
    ]
    assert df.shape == (1, 7)

def test_filter_cities_with_language():
    df = pd.DataFrame([
        {"CountryCode": "CO1", "LanguageCode": "EN"},
        {"CountryCode": "CO1", "LanguageCode": "FR"},
        {"CountryCode": "CO2", "LanguageCode": "FR"}
    ])
    result = filter_cities_with_language(df, "EN")
    assert result == ["CO1"]

def test_process_city_data_workflow(monkeypatch):
    fake = [
        {
            "CountryCode": "COX",
            "CityCode": "CX",
            "LanguageCode": "EN",
            "CityName": "NameX",
            "UtcOffset": "+00:00",
            "TimeZoneId": "TZX",
            "AirportCode": "AX"
        }
    ]
    monkeypatch.setattr(
        "api.data.city_data.fetch_city_data",
        lambda url, headers: {"dummy": True}
    )
    monkeypatch.setattr(
        "api.data.city_data.process_city_data",
        lambda data: fake
    )
    df = process_city_data_workflow({}, ["u1", "u2", "u3"])
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 3
    assert all(df["CityCode"] == "CX")
