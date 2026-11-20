"""Développeur 2 · historique Vélo'v (Lyon) avant novembre 2024, depuis l'API open data de la Métropole de Lyon.

CityBikes ne publie son historique que depuis novembre 2024. Pour Lyon, la Métropole publie l'historique des
disponibilités de chaque station (« jcd_jcdecaux.historiquevelov »). Ce script le télécharge jour par jour et l'écrit
au même format que les fichiers CityBikes :

  raw/citybikes/history/year=2024/month=M/2024MM-velov-stats.parquet   (id, name, latitude, longitude, bikes, free, timestamp)

`python -m pipelines.streaming_citybikes.history --no-download` le transforme ensuite en station_hourly et
mobility_city_hourly comme le reste. Le numéro de station Grand Lyon (`number`) est l'`uid` CityBikes : on reprend
l'identifiant, le nom et la position de la station dans le premier fichier CityBikes (novembre 2024).

Paris et Lille n'ont pas d'historique public équivalent : leurs données commencent en novembre 2024.

    python -m pipelines.streaming_citybikes.velov_history                       # janvier → octobre 2024
    python -m pipelines.streaming_citybikes.velov_history --start 2024-06-01 --end 2024-06-30
"""
import argparse
import json
import time
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from common import config

API = "https://data.grandlyon.com/fr/datapusher/ws/timeseries/jcd_jcdecaux.historiquevelov/all.json"
HIST = config.LOCAL_LAKE / "raw" / "citybikes" / "history"
CACHE = config.LOCAL_LAKE / "raw" / "grandlyon" / "velov_days"     # une journée brute par fichier (reprise possible)


def station_ref() -> pd.DataFrame:
    ref = pq.read_table(HIST / "year=2024/month=11/202411-velov-stats.parquet", columns=["id", "name", "latitude", "longitude", "extra"]).to_pandas()
    ref["uid"] = ref["extra"].map(lambda e: str(json.loads(e).get("uid")) if e else None)
    return ref.dropna(subset=["uid"]).drop_duplicates("uid", keep="last").set_index("uid")


def fetch_day(session: requests.Session, d: date) -> pd.DataFrame:
    out = CACHE / f"{d}.parquet"
    if out.exists():
        return pd.read_parquet(out)
    url = f"{API}?maxfeatures=100000&horodate__gte={d}T00:00:00&horodate__lt={d + timedelta(days=1)}T00:00:00"
    rows = []
    while url:
        for attempt in range(5):
            try:
                r = session.get(url, timeout=180)
                r.raise_for_status()
                js = r.json()
                break
            except Exception as e:  # noqa: BLE001 - réseau : on réessaie
                print(f"  {d} : nouvel essai ({e})", flush=True)
                time.sleep(10 * (attempt + 1))
        else:
            raise RuntimeError(f"{d} : échec du téléchargement")
        for v in js["values"]:
            a = (v.get("total_stands") or {}).get("availabilities") or {}
            rows.append((str(v["number"]), v["horodate"], v.get("status"), a.get("bikes"), a.get("stands")))
        url = js.get("next") if js["values"] else None
    df = pd.DataFrame(rows, columns=["uid", "horodate", "status", "bikes", "free"])
    CACHE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out)
    return df


def write_month(y: int, m: int, days: list[pd.DataFrame], ref: pd.DataFrame) -> None:
    df = pd.concat(days, ignore_index=True)
    df = df[df.uid.isin(ref.index) & (df.status == "OPEN")].copy()
    df["timestamp"] = pd.to_datetime(df.horodate, utc=True).dt.tz_convert(None)      # UTC, comme les fichiers CityBikes
    df = df.join(ref[["id", "name", "latitude", "longitude"]], on="uid").sort_values(["id", "timestamp"])
    df = df[(df.groupby("id")[["bikes", "free"]].shift() != df[["bikes", "free"]]).any(axis=1)]   # seulement les changements
    tbl = pa.Table.from_pandas(pd.DataFrame({
        "tag": "velov", "id": df["id"], "nuid": df["uid"], "name": df["name"], "latitude": df["latitude"], "longitude": df["longitude"],
        "bikes": df["bikes"].astype("int32"), "free": df["free"].astype("int32"), "extra": None,
        "timestamp": df["timestamp"].astype("datetime64[us]")}), preserve_index=False)
    d = HIST / f"year={y}" / f"month={m}"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, d / f"{y}{m:02d}-velov-stats.parquet")
    print(f"{y}-{m:02d} : {len(df):,} changements, {df['id'].nunique()} stations", flush=True)


def run(start: date, end: date) -> None:
    ref = station_ref()
    session = requests.Session()
    session.headers["Accept-Encoding"] = "gzip"
    months: dict[tuple[int, int], list] = {}
    d = start
    while d <= end:
        months.setdefault((d.year, d.month), []).append(fetch_day(session, d))
        nxt = d + timedelta(days=1)
        if nxt.month != d.month or nxt > end:
            write_month(d.year, d.month, months.pop((d.year, d.month)), ref)
        d = nxt


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2024-10-31", help="CityBikes prend le relais le 1er novembre 2024")
    a = ap.parse_args()
    run(date.fromisoformat(a.start), date.fromisoformat(a.end))
