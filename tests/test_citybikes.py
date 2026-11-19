"""Développeur 2 · tests du pipeline CityBikes (nettoyage, usage, contrat de données).  pytest tests/test_citybikes.py"""
import gzip
import json
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from pipelines.streaming_citybikes import clean, curate
from pipelines.streaming_citybikes.collector import raw_key

CLEAN_COLS = {"station_id", "station_name", "timestamp", "latitude", "longitude", "bikes_available", "free_slots",
              "city", "source", "ingestion_time"}
CURATED_COLS = ["city", "timestamp", "bikes_available", "free_slots", "capacity", "occupancy_rate", "usage_events",
                "nb_stations", "nb_empty_stations", "nb_full_stations"]


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def station(sid, bikes, slots, ts, lat=48.8566, lon=2.3522):
    return {"id": sid, "name": f" Station {sid} ", "latitude": lat, "longitude": lon,
            "timestamp": ts, "free_bikes": bikes, "empty_slots": slots, "extra": {"ebikes": 1}}


def write_raw(tmp_path, at: str, stations):
    body = json.dumps({"network": {"id": "velib", "stations": stations}}).encode()
    p = tmp_path / raw_key("velib", datetime.strptime(at, "%Y%m%dT%H%M%SZ"))
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(gzip.compress(body))


@pytest.fixture
def clean_df(spark, tmp_path):
    write_raw(tmp_path, "20261118T100000Z", [station("a", 10, 10, "2026-11-18T10:00:00.000000+00:00Z"),
                                            station("far", 5, 5, "2026-11-18T10:00:00.000000+00:00Z", lat=46.0, lon=2.0)])
    # même relevé renvoyé par le cache de l'API -> doublon
    write_raw(tmp_path, "20261118T100100Z", [station("a", 10, 10, "2026-11-18T10:00:00.000000+00:00Z")])
    write_raw(tmp_path, "20261118T100300Z", [station("a", 7, 13, "2026-11-18T10:03:00.000000+00:00Z")])
    write_raw(tmp_path, "20261118T100600Z", [station("a", 9, 11, "2026-11-18T10:06:00.000000+00:00Z")])
    # trou de 54 min (collecteur arrêté) : la variation ne doit pas compter comme de l'usage
    write_raw(tmp_path, "20261118T110000Z", [station("a", 0, 20, "2026-11-18T11:00:00.000000+00:00Z")])
    raw = (spark.read.option("multiLine", True).option("recursiveFileLookup", True).json(str(tmp_path / "citybikes"))
           .withColumn("_file", __import__("pyspark.sql.functions", fromlist=["x"]).input_file_name()))
    return clean.transform(raw)


def test_clean_respecte_le_contrat(clean_df):
    assert CLEAN_COLS <= set(clean_df.columns)
    types = dict(clean_df.dtypes)
    assert types["timestamp"] == "timestamp" and types["ingestion_time"] == "timestamp"
    assert types["latitude"] == "double" and types["longitude"] == "double"
    row = clean_df.filter("station_id = 'a'").orderBy("timestamp").first()
    assert row.city == "paris" and row.source == "citybikes" and row.station_name == "Station a"
    assert row.capacity == 20 and row.occupancy_rate == 0.5


def test_clean_dedoublonne_et_ville_hors_zone(clean_df):
    assert clean_df.filter("station_id = 'a'").count() == 4           # 5 dépôts, 1 doublon de cache
    assert clean_df.filter("station_id = 'far'").first().city is None  # à plus de 30 km de toute ville


def test_usage_ignore_les_trous(clean_df):
    st = curate.station_hourly(clean_df).filter("station_id = 'a'").orderBy("timestamp").collect()
    assert st[0].usage_events == 3 + 2                                 # 10 -> 7 -> 9
    assert st[1].usage_events == 0                                     # 9 -> 0 après 54 min : non compté
    assert st[1].is_empty and not st[0].is_full


def test_curated_ville_heure(clean_df):
    city = curate.city_hourly(curate.station_hourly(clean_df))
    assert city.columns == CURATED_COLS
    assert dict(city.dtypes)["timestamp"] == "timestamp"
    first = city.orderBy("timestamp").first()
    assert first.city == "paris" and first.timestamp.minute == 0 and first.nb_stations == 1


def test_raw_key_partitionne_par_date():
    assert raw_key("velib", datetime(2026, 11, 18, 14, 30)) == \
        "citybikes/networks/year=2026/month=11/day=18/velib_20261118T143000Z.json.gz"
