"""Tests du collecteur OpenAQ, sans appel réseau."""

from datetime import datetime, timezone

from pipelines.iot_openaq.collector import (
    locations_key,
    month_chunks,
    select_sensors,
    sensor_hours_key,
)

UTC = timezone.utc


def _location(loc_id, lat, lon, last="2026-11-18T10:00:00Z", params=("pm25", "no2"),
              is_monitor=True, is_mobile=False):
    return {
        "id": loc_id,
        "isMonitor": is_monitor,
        "isMobile": is_mobile,
        "name": f"station {loc_id}",
        "coordinates": {"latitude": lat, "longitude": lon},
        "datetimeLast": {"utc": last},
        "sensors": [
            {"id": loc_id * 10 + i, "parameter": {"name": p, "units": "µg/m³"}}
            for i, p in enumerate(params)
        ],
    }


SINCE = datetime(2024, 1, 1, tzinfo=UTC)


def test_select_sensors_keeps_contract_pollutants_only():
    locs = [_location(1, 48.86, 2.35, params=("pm25", "so2", "no2", "temperature"))]
    sensors = select_sensors(locs, "paris", SINCE)
    assert [s["pollutant"] for s in sensors] == ["pm25", "no2"]
    assert sensors[0] == {
        "sensor_id": 10,
        "location_id": 1,
        "location_name": "station 1",
        "pollutant": "pm25",
        "unit": "µg/m³",
    }


def test_select_sensors_drops_stations_outside_city():
    # Station à Lyon renvoyée par erreur dans la recherche de Paris.
    locs = [_location(1, 48.86, 2.35), _location(2, 45.76, 4.84)]
    assert {s["location_id"] for s in select_sensors(locs, "paris", SINCE)} == {1}


def test_select_sensors_drops_stations_inactive_since_2024():
    locs = [_location(1, 48.86, 2.35, last="2021-05-01T00:00:00Z")]
    assert select_sensors(locs, "paris", SINCE) == []


def test_select_sensors_drops_low_cost_and_mobile_sensors():
    locs = [
        _location(1, 48.86, 2.35),
        _location(2, 48.86, 2.35, is_monitor=False),  # AirGradient, HabitatMap...
        _location(3, 48.86, 2.35, is_mobile=True),
    ]
    assert {s["location_id"] for s in select_sensors(locs, "paris", SINCE)} == {1}


def test_month_chunks_cover_period_without_gap():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 4, 1, tzinfo=UTC)
    assert month_chunks(start, end) == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)),
        (datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)),
        (datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)),
    ]


def test_month_chunks_three_years():
    chunks = month_chunks(datetime(2024, 1, 1, tzinfo=UTC), datetime(2026, 11, 1, tzinfo=UTC))
    assert len(chunks) == 34
    assert all(a < b for a, b in chunks)
    assert all(chunks[i][1] == chunks[i + 1][0] for i in range(len(chunks) - 1))


def test_raw_keys_follow_lake_layout():
    now = datetime(2026, 11, 19, 14, 5, 0, tzinfo=UTC)
    assert locations_key("lyon", now) == (
        "openaq/locations/year=2026/month=11/day=19/lyon_20261119T140500Z.json.gz"
    )
    start, end = datetime(2024, 2, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)
    assert sensor_hours_key(42, start, end) == (
        "openaq/sensor_hours/year=2024/month=02/day=01/"
        "sensor_42_20240201T000000Z_20240301T000000Z.json.gz"
    )
