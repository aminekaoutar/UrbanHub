"""Développeur 2 · trajets V'Lille (Lille) : chaque location depuis avril 2025, publiée par ilévia / la MEL.

Source : https://data.lillemetropole.fr (jeu « ilévia - Emprunts V'lille », OGC API Features), Licence Ouverte 2.0.
Contrairement à CityBikes (état des stations), c'est un vrai trajet : départ, arrivée, stations, heures.

  raw   : raw/ilevia/vlille_trips/page_<n>.json.gz               réponses de l'API telles quelles
  clean : clean/ilevia/vlille_trips/year=/month=/part.parquet     une ligne par trajet (UTC) + position des stations

    python -m pipelines.streaming_citybikes.vlille_trips          # télécharge les pages manquantes puis refait clean
"""
import gzip
import json
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from common import config

URL = "https://data.lillemetropole.fr/geoserver/ogc/features/v1/collections/dsp_ilevia:emprunt/items"
PAGE = 50000
RAW = config.LOCAL_LAKE / "raw" / "ilevia" / "vlille_trips"
CLEAN = config.LOCAL_LAKE / "clean" / "ilevia" / "vlille_trips"


def download() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["Accept-Encoding"] = "gzip"
    n = 0
    while True:
        f = RAW / f"page_{n:04d}.json.gz"
        last = n == len(list(RAW.glob("page_*.json.gz"))) - 1     # la dernière page grossit : on la reprend
        if not f.exists() or last:
            for attempt in range(5):
                try:
                    r = session.get(URL, params={"f": "application/json", "limit": PAGE, "startIndex": n * PAGE}, timeout=180)
                    r.raise_for_status()
                    json.loads(r.content)
                    break
                except Exception as e:  # noqa: BLE001 - réseau : on réessaie
                    print(f"page {n} : nouvel essai ({e})", flush=True)
                    time.sleep(10 * (attempt + 1))
            else:
                raise RuntimeError(f"page {n} : échec")
            f.write_bytes(gzip.compress(r.content))
        got = len(json.loads(gzip.decompress(f.read_bytes()))["features"])
        print(f"page {n} : {got} trajets", flush=True)
        if got < PAGE:
            break
        n += 1


def clean() -> None:
    rows = [ft["properties"] for f in sorted(RAW.glob("page_*.json.gz")) for ft in json.loads(gzip.decompress(f.read_bytes()))["features"]]
    df = pd.DataFrame(rows).drop_duplicates("id_emprunt")
    out = pd.DataFrame({
        "trip_id": df.id_emprunt.astype("int64"),
        "start_time": pd.to_datetime(df.date_heure_debut, utc=True), "end_time": pd.to_datetime(df.date_heure_arrivee, utc=True),
        "start_station_id": df.id_station_depart.astype("Int64"), "start_station": df.nom_station_depart.str.strip(),
        "end_station_id": df.id_station_arrivee.astype("Int64"), "end_station": df.nom_station_arrivee.str.strip(),
        "start_commune": df.commune_depart, "end_commune": df.commune_arrivee,
    })
    out["duration_min"] = (out.end_time - out.start_time).dt.total_seconds() / 60
    out.loc[(out.duration_min < 0) | (out.duration_min > 24 * 60), "duration_min"] = None      # valeurs impossibles → null
    # position des stations : même numéro que l'uid CityBikes de V'Lille
    files = sorted(config.LOCAL_LAKE.glob("raw/citybikes/history/year=*/month=*/*-vlille-stats.parquet"))
    pos = pq.read_table(files[-1], columns=["nuid", "latitude", "longitude"]).to_pandas().drop_duplicates("nuid", keep="last")
    pos = pos.assign(nuid=pd.to_numeric(pos.nuid, errors="coerce")).dropna().set_index("nuid")
    for side in ("start", "end"):
        out[f"{side}_lat"] = out[f"{side}_station_id"].map(pos.latitude)
        out[f"{side}_lon"] = out[f"{side}_station_id"].map(pos.longitude)
    out["city"], out["source"] = "lille", "ilevia"
    out["ingestion_time"] = pd.Timestamp.now(tz="UTC").floor("s")
    for (y, m), g in out.groupby([out.start_time.dt.year, out.start_time.dt.month]):
        d = CLEAN / f"year={y}" / f"month={m}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(g, preserve_index=False), d / "part.parquet")
    print(f"clean : {len(out):,} trajets, {out.start_time.min()} → {out.start_time.max()}", flush=True)


if __name__ == "__main__":
    download()
    clean()
