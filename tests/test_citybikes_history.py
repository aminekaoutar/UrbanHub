"""Tests de pipelines/streaming_citybikes/history.py (historique data.citybik.es), sans Spark ni réseau."""
import pandas as pd
import pytest

from pipelines.streaming_citybikes.history import city_hourly, station_hourly, to_clean

PARIS, LYON = (48.8566, 2.3522), (45.7640, 4.8357)
START = pd.Timestamp("2025-01-01", tz="UTC")
END = pd.Timestamp("2025-02-01", tz="UTC")
NOW = pd.Timestamp("2026-11-20", tz="UTC")


def raw(rows):
    """rows : (id, heure 'HH:MM', vélos, places libres[, (lat, lon)])"""
    return pd.DataFrame([{
        "id": r[0], "name": f" station {r[0]} ", "latitude": (r[4] if len(r) > 4 else PARIS)[0],
        "longitude": (r[4] if len(r) > 4 else PARIS)[1], "bikes": r[2], "free": r[3],
        "timestamp": pd.Timestamp(f"2025-01-01 {r[1]}")} for r in rows])


def test_to_clean_contract_columns():
    df = to_clean(raw([("a", "00:10", 3, 7), ("b", "00:20", -1, 5, LYON), ("c", "00:30", 1, 1, (43.3, 5.4))]),
                  "velib", NOW)
    a, b, c = (df[df.station_id == s].iloc[0] for s in "abc")
    assert str(df["timestamp"].dt.tz) == "UTC" and a.station_name == "station a"
    assert (a.city, a.capacity, a.occupancy_rate, a.source) == ("paris", 10, 0.3, "citybikes")
    assert b.city == "lyon" and pd.isna(b.bikes_available)          # valeur impossible → null
    assert pd.isna(c.city)                                            # Marseille : hors des 3 villes


def test_hourly_weighted_state_and_exact_usage():
    carry = to_clean(raw([("a", "00:00", 5, 5)]), "velib", NOW)
    ev = to_clean(raw([("a", "00:30", 3, 7), ("a", "02:10", 4, 6)]), "velib", NOW)
    st = station_hourly(ev, carry, START, END).set_index("timestamp")
    assert len(st) == 3                                                # 00 h, 01 h, 02 h ; rien après le dernier fichier
    h0, h1, h2 = (st.loc[START + pd.Timedelta(hours=h)] for h in range(3))
    assert (h0.bikes_available, h0.usage_events, h0.nb_readings) == (4.0, 2, 1)   # 30 min à 5 puis 30 min à 3
    assert (h1.bikes_available, h1.usage_events, h1.nb_readings) == (3.0, 0, 0)   # aucun changement : état reporté
    assert (h2.bikes_available, h2.usage_events) == (3.83, 1)                     # 10 min à 3, 50 min à 4
    assert h0.occupancy_rate == pytest.approx(0.4) and not h0.is_empty


def test_source_outage_not_filled():
    ev = to_clean(raw([("a", "00:30", 3, 7), ("a", "05:10", 9, 1)]), "velib", NOW)
    st = station_hourly(ev, None, START, END)
    hours = set(st["timestamp"].dt.hour)
    assert hours == {5}                                                 # 4 h 40 sans aucun changement : panne
    assert st["usage_events"].sum() == 0                                # pas de faux pic d'usage après la panne


def test_city_hourly_matches_contract():
    ev = to_clean(raw([("a", "00:00", 0, 10), ("a", "00:59", 0, 10), ("b", "00:00", 10, 0), ("c", "00:00", 5, 5, LYON)]), "velib", NOW)
    city = city_hourly(station_hourly(ev, None, START, END)).set_index("city")
    p = city.loc["paris"]
    assert (p.bikes_available, p.capacity, p.nb_stations, p.nb_empty_stations, p.nb_full_stations) == (10, 20, 2, 1, 1)
    assert p.occupancy_rate == 0.5 and city.loc["lyon"].nb_stations == 1
