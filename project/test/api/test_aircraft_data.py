import pytest
import requests
import pandas as pd

from api.data.aircraft_data import (
    fetch_aircraft_data,
    create_aircraft_dataframe,
    process_aircraft_data_workflow
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

def test_fetch_aircraft_data_success(monkeypatch):
    sample_json = {
        "AircraftResource": {
            "AircraftSummaries": {
                "AircraftSummary": [
                    {
                        "AircraftCode": "A320",
                        "AirlineEquipCode": "320",
                        "Names": {
                            "Name": [
                                {"@LanguageCode": "EN", "$": "Airbus A320"},
                                {"@LanguageCode": "FR", "$": "Airbus A320 FR"}
                            ]
                        }
                    },
                    {
                        "AircraftCode": "B737",
                        "AirlineEquipCode": "737",
                        "Names": {}  # 测试无 Names 分支
                    }
                ]
            }
        }
    }
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(sample_json, status_code=200)
    )

    result = fetch_aircraft_data("http://fake.url", headers={"X": "Y"})

    assert isinstance(result, list)
    assert any(r["LanguageCode"] == "EN" and r["AircraftName"] == "Airbus A320"
               for r in result)
    assert any(r["LanguageCode"] == "FR" for r in result)
    assert any(r["AircraftCode"] == "B737" and r["AircraftName"] is None
               for r in result)

def test_fetch_aircraft_data_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse({}, status_code=500)
    )

    res = fetch_aircraft_data("http://fake.url", headers={})
    assert res == []
    captured = capsys.readouterr()
    assert "Error fetching data" in captured.out

def test_create_aircraft_dataframe():
    data = [
        {"AircraftCode": "X1", "LanguageCode": "EN", "AircraftName": "X One", "AirlineEquipCode": "X"},
        {"AircraftCode": "Y2", "LanguageCode": "FR", "AircraftName": "Y Deux", "AirlineEquipCode": "Y"},
    ]

    df = create_aircraft_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["AircraftCode", "LanguageCode", "AircraftName", "AirlineEquipCode"]
    assert df.shape == (2, 4)

def test_process_aircraft_data_workflow(monkeypatch):
    fake = [
        {"AircraftCode": "Z1", "LanguageCode": "EN", "AircraftName": "Zed One", "AirlineEquipCode": "Z"}
    ]
    monkeypatch.setattr(
        "api.data.aircraft_data.fetch_aircraft_data",
        lambda url, headers: fake
    )

    headers = {"Auth": "token"}
    urls = ["u1", "u2", "u3"]
    df = process_aircraft_data_workflow(headers, urls)

    assert df.shape[0] == 3
    assert (df["AircraftCode"] == "Z1").all()