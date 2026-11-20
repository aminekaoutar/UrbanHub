"""Tests du site web (dashboard/webapp) sur un petit Data Lake fabriqué pour le test, sans Spark ni réseau."""
import importlib
import sys

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("duckdb")
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

HOURS = pd.date_range("2025-03-01", periods=24 * 40, freq="h", tz="UTC")     # 40 jours
CITIES = ["paris", "lyon", "lille"]


def write(lake, rel, df):
    d = lake / rel / "year=2025" / "month=3"
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "part.parquet", index=False)


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    rng = np.random.default_rng(0)
    rows = lambda: pd.DataFrame([(c, t) for c in CITIES for t in HOURS], columns=["city", "timestamp"])  # noqa: E731
    w = rows()
    w["temperature"] = 10 + 8 * np.sin(np.arange(len(w)) / 24 * 2 * np.pi) + rng.normal(0, 1, len(w))
    w.loc[w.timestamp.dt.day == 15, "temperature"] += 15                                      # une journée très chaude
    w["wind_speed"], w["wind_direction"], w["pressure"] = rng.uniform(0, 8, len(w)), 180.0, 1015.0
    w["precipitation"] = np.where(rng.uniform(size=len(w)) < 0.1, 3.0, 0.0)
    w["visibility"], w["nb_stations"] = 20000.0, 1
    write(lake, "curated/openmeteo/weather_city_hourly", w)
    m = rows()
    m["bikes_available"], m["free_slots"], m["capacity"] = 500, 500, 1000
    m["occupancy_rate"], m["usage_events"] = 0.5, (m.timestamp.dt.hour * 10).astype("int32")
    m["nb_stations"], m["nb_empty_stations"], m["nb_full_stations"] = 100, 10, 5
    write(lake, "curated/citybikes/mobility_city_hourly", m)
    s = pd.DataFrame([(f"{i:032x}", f"Station {i}", c, t) for i, c in enumerate(CITIES * 4) for t in HOURS[: 24 * 10]],
                     columns=["station_id", "station_name", "city", "timestamp"])
    s["latitude"], s["longitude"], s["capacity"] = 48.85, 2.35, 20.0
    s["occupancy_rate"], s["usage_events"] = 0.4, 3
    s["is_empty"], s["is_full"] = False, False
    write(lake, "curated/citybikes/station_hourly", s)
    p = rows()
    for k in ("pm25", "pm10", "no2", "o3", "co"):
        p[k] = rng.uniform(5, 30, len(p))
    p["nb_sensors"] = 3
    write(lake, "curated/openaq/pollution_city_hourly", p)
    me = pd.DataFrame({"sensor_id": 1, "timestamp": HOURS, "latitude": 48.85, "longitude": 2.35, "pollutant": "no2", "value": 20.0,
                       "unit": "µg/m³", "city": "paris", "source": "openaq", "ingestion_time": HOURS})
    write(lake, "clean/openaq/measurements", me)
    import os
    old = os.environ.get("LOCAL_LAKE")
    os.environ["LOCAL_LAKE"] = str(lake)
    sys.modules.pop("dashboard.webapp.app", None)
    app = importlib.import_module("dashboard.webapp.app")
    yield TestClient(app.app)
    sys.modules.pop("dashboard.webapp.app", None)
    if old is None:
        os.environ.pop("LOCAL_LAKE", None)
    else:
        os.environ["LOCAL_LAKE"] = old


def test_meta_lists_tables(client):
    meta = client.get("/api/meta").json()
    names = {t["name"]: t["rows"] for t in meta["tables"]}
    assert names["weather_city_hourly"] == 3 * len(HOURS) and "station_hourly" in names
    assert meta["has_trips"] is False


@pytest.mark.parametrize("path", [
    "/api/coverage", "/api/weather/daily?city=paris", "/api/weather/monthly", "/api/weather/extremes", "/api/weather/visibility",
    "/api/mobility/daily?city=lyon", "/api/mobility/profile?city=paris", "/api/mobility/stations?city=paris", "/api/mobility/heatmap?city=lille",
    "/api/mobility/monthly", "/api/pollution/daily?city=paris", "/api/pollution/dominant", "/api/pollution/stations",
    "/api/pollution/episodes?city=paris&pollutant=no2", "/api/cross/daily", "/api/table/station_hourly?limit=5"])
def test_endpoints_answer_with_rows_and_sql(client, path):
    r = client.get(path)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rows"] and body["sql"].strip().upper().startswith(("SELECT", "WITH"))


def test_hot_day_is_extreme_and_abnormal(client):
    hot = client.get("/api/weather/extremes", params={"heat": 30}).json()["rows"]
    assert any(r["day"] == "2025-03-15" and r["type"] == "Heat" for r in hot)
    ano = client.get("/api/weather/anomalies", params={"z": 2, "min_days": 1}).json()["rows"]
    assert any(r["start"] <= "2025-03-15" <= r["end"] and r["type"] == "Hot" for r in ano)


def test_period_filter(client):
    rows = client.get("/api/mobility/daily", params={"city": "paris", "start": "2025-03-10", "end": "2025-03-12"}).json()["rows"]
    assert [r["day"] for r in rows] == ["2025-03-10", "2025-03-11", "2025-03-12"]


def test_bad_input_is_refused(client):
    assert client.get("/api/weather/daily?city=rome").status_code == 400
    assert client.get("/api/weather/daily?city=paris&start=2025-13").status_code == 400
    assert client.get("/api/pollution/episodes?city=paris&pollutant=x").status_code == 400
    assert client.get("/api/table/secrets").status_code == 404
    assert client.get("/api/trips/summary").status_code == 404          # pas de trajets dans ce petit lac
