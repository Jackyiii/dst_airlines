# tests/test_schedules_data.py

import pytest
import requests
import pandas as pd

from api.data.schedules_data import (
    fetch_schedules_data,
    process_schedule,
    create_schedules_dataframe,
    process_schedules_workflow
)

class DummyResponse:
    def __init__(self, json_data=None, status_code=200, json_exc=None):
        self._json = json_data or {}
        self.status_code = status_code
        self._json_exc = json_exc

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._json

def test_process_schedule_full():
    schedule = {
        "TotalJourney": {"Duration": "PT2H"},
        "Flight": {
            "Departure": {
                "AirportCode": "AAA",
                "ScheduledTimeLocal": {"DateTime": "2025-05-11T10:00"},
                "Terminal": {"Name": "T1"},
            },
            "Arrival": {
                "AirportCode": "BBB",
                "ScheduledTimeLocal": {"DateTime": "2025-05-11T12:00"},
                "Terminal": {"Name": "T2"},
            },
            "MarketingCarrier": {"AirlineID": "LH", "FlightNumber": "400"},
            "Equipment": {"AircraftCode": "A320"},
            "Details": {
                "Stops": {"StopQuantity": 0},
                "DaysOfOperation": "Daily",
                "DatePeriod": {"Effective": "2025-01-01", "Expiration": "2025-12-31"}
            }
        }
    }
    result = process_schedule(schedule)
    assert result["Duration"] == "PT2H"
    assert result["DepartureAirportCode"] == "AAA"
    assert result["ArrivalAirportCode"] == "BBB"
    assert result["AirlineID"] == "LH"
    assert result["FlightNumber"] == "400"
    assert result["AircraftCode"] == "A320"
    assert result["StopQuantity"] == 0
    assert result["DaysOfOperation"] == "Daily"
    assert result["EffectiveDate"] == "2025-01-01"
    assert result["ExpirationDate"] == "2025-12-31"

def test_fetch_schedules_data_success(monkeypatch):
    sample = {
        "ScheduleResource": {
            "Schedule": [
                {"TotalJourney": {"Duration": "PT1H"}, "Flight": {}},
                {"TotalJourney": {"Duration": "PT2H"}, "Flight": {}}
            ]
        }
    }
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data=sample, status_code=200)
    )
    schedules, failed = fetch_schedules_data("http://fake", headers={})
    assert len(schedules) == 2
    assert failed == []

def test_fetch_schedules_data_nonlist(monkeypatch, capsys):
    sample = {"ScheduleResource": {"Schedule": {"not": "a list"}}}
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data=sample, status_code=200)
    )
    schedules, failed = fetch_schedules_data("http://fake", headers={})
    assert schedules == []
    assert failed == ["http://fake"]

def test_fetch_schedules_data_status_error(monkeypatch):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data={}, status_code=404)
    )
    schedules, failed = fetch_schedules_data("http://fake", headers={})
    assert schedules == []
    assert failed == ["http://fake"]

def test_fetch_schedules_data_json_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data=None, status_code=200, json_exc=ValueError())
    )
    schedules, failed = fetch_schedules_data("http://fake", headers={})
    assert schedules == []
    assert failed == ["http://fake"]
    out = capsys.readouterr().out
    assert "Erreur lors de l'analyse de la réponse JSON" in out

def test_create_schedules_dataframe():
    info = [
        {"Duration": "PT1H", "FlightNumber": "X1"},
        {"Duration": "PT2H", "FlightNumber": "X2"}
    ]
    df = create_schedules_dataframe(info)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["Duration", "FlightNumber"]
    assert df.shape == (2, 2)

def test_process_schedules_workflow(monkeypatch):
    fake_info = [{"Duration": "PT3H", "FlightNumber": "X3"}]
    monkeypatch.setattr(
        "api.data.schedules_data.fetch_schedules_data",
        lambda url, headers: (fake_info, ["bad"])
    )
    df, failed = process_schedules_workflow({}, ["u1", "u2"])
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert failed == ["bad", "bad"]
    assert all(df["Duration"] == "PT3H")
