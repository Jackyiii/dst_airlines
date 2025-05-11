# tests/test_airport_data.py

import pytest
import requests
import pandas as pd

from api.data.airport_data import (
    fetch_airport_data,
    create_airport_dataframe,
    filter_airports_with_language,
    process_airport_data_workflow
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

def test_fetch_airport_data_success(monkeypatch):
    sample_json = {
        "AirportResource": {
            "Airports": {
                "Airport": [
                    {
                        "AirportCode": "AAA",
                        "CityCode": "CCC",
                        "CountryCode": "KKK",
                        "LocationType": "Type1",
                        "UtcOffset": "+01:00",
                        "TimeZoneId": "Europe/Paris",
                        "Names": {"Name": {"@LanguageCode": "EN", "$": "Airport EN"}},
                        "Position": {"Coordinate": {"Latitude": 1.23, "Longitude": 4.56}}
                    },
                    {
                        "AirportCode": "BBB",
                        "CityCode": "DDD",
                        "CountryCode": "LLL",
                        "LocationType": "Type2",
                        "UtcOffset": "+02:00",
                        "TimeZoneId": "Europe/Berlin",
                        "Names": {"Name": [
                            {"@LanguageCode": "EN", "$": "Airport2 EN"},
                            {"@LanguageCode": "FR", "$": "Airport2 FR"}
                        ]},
                        "Position": {"Coordinate": {"Latitude": 7.89, "Longitude": 0.12}}
                    }
                ]
            }
        }
    }

    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(sample_json, status_code=200)
    )

    result = fetch_airport_data("http://fake.url", headers={"X": "Y"})
    assert isinstance(result, list)
    # Should produce 1 + 2 = 3 entries
    assert len(result) == 3
    # Check first entry
    first = result[0]
    assert first["AirportCode"] == "AAA"
    assert first["Latitude"] == 1.23 and first["Longitude"] == 4.56
    assert first["LanguageCode"] == "EN" and first["AirportName"] == "Airport EN"
    # Check one of the second airport's entries
    assert any(r["AirportCode"] == "BBB" and r["LanguageCode"] == "FR" for r in result)

def test_fetch_airport_data_http_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse({}, status_code=500)
    )

    res = fetch_airport_data("http://fake.url", headers={})
    assert res == []
    captured = capsys.readouterr()
    assert "Error fetching data" in captured.out

def test_create_airport_dataframe():
    data = [
        {
            "AirportCode": "X1", "Latitude": 0.1, "Longitude": 0.2,
            "CityCode": "C1", "CountryCode": "CO1", "LocationType": "L1",
            "LanguageCode": "EN", "AirportName": "X One", "UtcOffset": "+00:00", "TimeZoneId": "TZ1"
        },
        {
            "AirportCode": "Y2", "Latitude": 1.1, "Longitude": 1.2,
            "CityCode": "C2", "CountryCode": "CO2", "LocationType": "L2",
            "LanguageCode": "FR", "AirportName": "Y Deux", "UtcOffset": "+01:00", "TimeZoneId": "TZ2"
        }
    ]
    df = create_airport_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "AirportCode", "Latitude", "Longitude", "CityCode", "CountryCode",
        "LocationType", "LanguageCode", "AirportName", "UtcOffset", "TimeZoneId"
    ]
    assert df.shape == (2, 10)

def test_filter_airports_with_language():
    df = pd.DataFrame([
        {"LanguageCode": "EN", "AirportCode": "A1"},
        {"LanguageCode": "FR", "AirportCode": "A2"},
        {"LanguageCode": "EN", "AirportCode": "A3"}
    ])
    filtered = filter_airports_with_language(df, "EN")
    assert isinstance(filtered, pd.DataFrame)
    assert set(filtered["AirportCode"]) == {"A1", "A3"}

def test_process_airport_data_workflow(monkeypatch):
    fake = [
        {"AirportCode": "Z1", "Latitude": 9.9, "Longitude": 8.8,
         "CityCode": "CZ", "CountryCode": "CZ", "LocationType": "LT",
         "LanguageCode": "EN", "AirportName": "Zed One",
         "UtcOffset": "+03:00", "TimeZoneId": "TZ"}
    ]
    monkeypatch.setattr(
        "api.data.airport_data.fetch_airport_data",
        lambda url, headers: fake
    )

    headers = {"Auth": "token"}
    urls = ["u1", "u2", "u3"]
    df = process_airport_data_workflow(headers, urls)
    assert df.shape[0] == 3
    assert all(df["AirportCode"] == "Z1")
    assert all(df["LanguageCode"] == "EN")
