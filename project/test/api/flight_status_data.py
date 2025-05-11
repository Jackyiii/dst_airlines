# tests/test_flight_status.py

import pytest
import requests
import pandas as pd
from api.data.flight_status import (
    extract_date,
    fetch_flight_status,
    process_flight_status,
    process_flight_status_workflow
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

def test_extract_date():
    assert extract_date("2025-05-11T15:40:59") == "2025-05-11"
    assert extract_date("2021-01-01T00:00:00Z") == "2021-01-01"

def test_fetch_flight_status_success(monkeypatch, capsys):
    fake_data = {"FlightStatusResource": {}}
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data=fake_data, status_code=200)
    )
    result = fetch_flight_status("LH400", "2025-05-11", headers={})
    assert result == fake_data
    out = capsys.readouterr().out
    assert "Statut récupéré avec succès" in out

def test_fetch_flight_status_json_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(json_data=None, status_code=200, json_exc=ValueError())
    )
    result = fetch_flight_status("LH400", "2025-05-11", headers={})
    assert result is None
    out = capsys.readouterr().out
    assert "Erreur lors de l'analyse de la réponse JSON" in out

def test_fetch_flight_status_http_error(monkeypatch, capsys):
    monkeypatch.setattr(
        requests, "get",
        lambda url, headers: DummyResponse(status_code=404)
    )
    result = fetch_flight_status("LH400", "2025-05-11", headers={})
    assert result is None
    out = capsys.readouterr().out
    assert "Erreur lors de la récupération des données" in out

def test_process_flight_status_basic():
    flight_status_data = {
        "FlightStatusResource": {
            "Flights": {
                "Flight": [
                    {
                        "Departure": {
                            "AirportCode": "AAA",
                            "ScheduledTimeLocal": {"DateTime": "2025-05-11T10:00"},
                            "ActualTimeLocal": {"DateTime": "2025-05-11T10:05"},
                            "Terminal": {"Name": "T1", "Gate": "G1"},
                            "TimeStatus": {"Code": "ON", "Definition": "On Time"}
                        },
                        "Arrival": {
                            "AirportCode": "BBB",
                            "ScheduledTimeLocal": {"DateTime": "2025-05-11T12:00"},
                            "ActualTimeLocal": {"DateTime": "2025-05-11T12:10"},
                            "Terminal": {"Name": "T2", "Gate": "G2"}
                        },
                        "MarketingCarrier": {"AirlineID": "LH", "FlightNumber": "400"},
                        "Equipment": {"AircraftCode": "A320", "AircraftRegistration": "D-AIXX"},
                        "FlightStatus": {"Code": "L", "Definition": "Landed"}
                    }
                ]
            }
        }
    }
    result = process_flight_status(flight_status_data, "LH400", "2025-05-11")
    assert isinstance(result, list)
    assert len(result) == 1
    entry = result[0]
    assert entry["FlightNumber"] == "LH400"
    assert entry["DepartureAirportCode"] == "AAA"
    assert entry["ScheduledDepartureLocalTime"] == "2025-05-11T10:00"
    assert entry["ActualArrivalLocalTime"] == "2025-05-11T12:10"
    assert entry["TimeStatusCode"] == "ON"
    assert entry["MarketingAirlineID"] == "LH"
    assert entry["AircraftCode"] == "A320"
    assert entry["FlightStatusDefinition"] == "Landed"

def test_process_flight_status_empty():
    empty_data = {"FlightStatusResource": {"Flights": {"Flight": []}}}
    result = process_flight_status(empty_data, "XX100", "2025-01-01")
    assert result == []

def test_process_flight_status_workflow(monkeypatch):
    df = pd.DataFrame([
        {"ScheduleID": "LH400", "DepartureTime": "2025-05-11T00:00:00"},
        {"ScheduleID": "LH401", "DepartureTime": "2025-05-12T00:00:00"}
    ])
    fake_status = [{"FlightNumber": "X", "DepartureDate": "2025-05-11"}]
    monkeypatch.setattr(
        "api.data.flight_status.fetch_flight_status",
        lambda num, date, headers: {"dummy": True}
    )
    monkeypatch.setattr(
        "api.data.flight_status.process_flight_status",
        lambda data, num, date: fake_status
    )
    result_df = process_flight_status_workflow(df, headers={})
    assert isinstance(result_df, pd.DataFrame)
    assert result_df.shape == (2, 2)  # one column per dict key in fake_status
    assert list(result_df.columns) == ["FlightNumber", "DepartureDate"]
    assert all(result_df["FlightNumber"] == "X")
    assert all(result_df["DepartureDate"] == "2025-05-11")
