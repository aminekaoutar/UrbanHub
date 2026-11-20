"""Développeur 2 · historique CityBikes (data.citybik.es) → raw → clean → curated.

Source : https://data.citybik.es, la même que le collecteur (mêmes station_id). Un fichier Parquet par réseau
et par mois : {AAAAMM}-{réseau}-stats.parquet, depuis novembre 2024.
Licence ODbL, citation obligatoire : « Bike-share data by CityBikes contributors and available from
https://data.citybik.es ».

Différence avec le collecteur : un fichier ne contient que les CHANGEMENTS d'état des stations (une ligne quand
le nombre de vélos change), pas un relevé toutes les 2 min. Donc, pour chaque station et chaque heure :
- l'état est le dernier état connu, moyenne pondérée par la durée (= moyenne des relevés de 2 min du collecteur) ;
- usage_events = somme EXACTE des |Δ bikes_available| des changements de l'heure.
Une panne de la source (aucun changement dans tout le réseau pendant plus de 2 h) n'est pas comblée :
les heures concernées sont retirées, comme quand le collecteur est arrêté.

Sorties, mois de l'historique seulement (un mois déjà couvert par le collecteur n'est jamais touché) :
  raw/citybikes/history/year=/month=/<fichier>.parquet           fichier tel que téléchargé
  clean/citybikes/station_events/year=/month=/part.parquet       une ligne = un changement (colonnes du contrat)
  curated/citybikes/station_hourly/year=/month=/...              mêmes colonnes que curate.py
  curated/citybikes/mobility_city_hourly/year=/month=/...        livraison du contrat (section 4.3)

Écrit dans le Data Lake local (data/) ; « python -m common.storage » pousse ensuite vers MinIO.

    python -m pipelines.streaming_citybikes.history                 # télécharge ce qui manque, puis traite tout
    python -m pipelines.streaming_citybikes.history --no-download
"""
import argparse
import shutil
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from common import config
from common.cities import assign_city

BASE_URL = "https://data.citybik.es/dumps/by-network"
NETWORKS = ("velib", "velov", "vlille")            # paris, lyon, lille : les réseaux du collecteur
RAW = "citybikes/history"
CLEAN_EVENTS = "citybikes/station_events"
LIVE_CLEAN = "citybikes/station_status"            # mois du collecteur : prioritaires, jamais réécrits
STATION_HOURLY = "citybikes/station_hourly"
CITY_HOURLY = "citybikes/mobility_city_hourly"
OUTAGE = pd.Timedelta(hours=2)                      # aucun changement dans tout le réseau → source en panne
HOUR = pd.Timedelta(hours=1)
EMPTY, FULL = 0.10, 0.90

TS = pa.timestamp("us", tz="UTC")
EVENTS_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("station_name", pa.string()), ("timestamp", TS),
    ("latitude", pa.float64()), ("longitude", pa.float64()),
    ("bikes_available", pa.int32()), ("free_slots", pa.int32()), ("city", pa.string()), ("source", pa.string()),
    ("ingestion_time", TS), ("network_id", pa.string()), ("capacity", pa.int32()), ("occupancy_rate", pa.float64()),
])
# types identiques à ceux écrits par curate.py (Spark), sinon Spark ne peut pas relire le dossier en entier
STATION_HOURLY_SCHEMA = pa.schema([
    ("station_id", pa.string()), ("timestamp", TS), ("station_name", pa.string()), ("network_id", pa.string()),
    ("city", pa.string()), ("latitude", pa.float64()), ("longitude", pa.float64()),
    ("bikes_available", pa.float64()), ("free_slots", pa.float64()), ("capacity", pa.float64()),
    ("occupancy_rate", pa.float64()), ("usage_events", pa.int32()), ("nb_readings", pa.int64()),
    ("is_empty", pa.bool_()), ("is_full", pa.bool_()),
])
CITY_HOURLY_SCHEMA = pa.schema([                    # = common/schemas.py MOBILITY_CITY_HOURLY
    ("city", pa.string()), ("timestamp", TS),
    ("bikes_available", pa.int32()), ("free_slots", pa.int32()), ("capacity", pa.int32()),
    ("occupancy_rate", pa.float64()), ("usage_events", pa.int32()),
    ("nb_stations", pa.int32()), ("nb_empty_stations", pa.int32()), ("nb_full_stations", pa.int32()),
])


# ---------- téléchargement (raw) ----------

def available_files() -> list[tuple[int, int, str]]:
    """(année, mois, nom) des fichiers publiés pour nos réseaux."""
    files = []
    for year in range(2024, datetime.now(timezone.utc).year + 1):
        r = requests.get(f"{BASE_URL}/{year}/", headers={"Accept": "application/json"}, timeout=60)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        for x in r.json():
            name = x["name"]
            if any(name.endswith(f"-{n}-stats.parquet") for n in NETWORKS):
                files.append((year, int(name[4:6]), name))
    return sorted(files)


def download() -> None:
    for year, month, name in available_files():
        dest = config.LOCAL_LAKE / "raw" / RAW / f"year={year}" / f"month={month}" / name
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(f"{BASE_URL}/{year}/{name}", stream=True, timeout=300) as r:
            r.raise_for_status()
            tmp = dest.with_suffix(".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
            tmp.rename(dest)
        print(f"téléchargé : {name}")


# ---------- raw → clean ----------

def to_clean(raw: pd.DataFrame, network: str, ingestion_time: pd.Timestamp) -> pd.DataFrame:
    df = raw.rename(columns={"id": "station_id", "name": "station_name",
                             "bikes": "bikes_available", "free": "free_slots"})
    ts = pd.to_datetime(df["timestamp"])
    df["timestamp"] = ts.dt.tz_localize("UTC") if ts.dt.tz is None else ts.dt.tz_convert("UTC")
    df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)].copy()
    for c in ("bikes_available", "free_slots"):                       # valeurs impossibles → null
        df[c] = df[c].where(df[c] >= 0).astype("Int32")
    df["station_name"] = df["station_name"].str.strip()
    coords = df[["latitude", "longitude"]].drop_duplicates()
    coords["city"] = [assign_city(a, b) for a, b in zip(coords["latitude"], coords["longitude"])]
    df = df.merge(coords, on=["latitude", "longitude"], how="left")
    df["capacity"] = (df["bikes_available"] + df["free_slots"]).astype("Int32")
    df["occupancy_rate"] = (df["bikes_available"] / df["capacity"].where(df["capacity"] > 0)).astype(float).round(4)
    df["source"], df["network_id"], df["ingestion_time"] = "citybikes", network, ingestion_time
    df = df.drop_duplicates(["station_id", "timestamp"]).sort_values(["station_id", "timestamp"])
    return df[EVENTS_SCHEMA.names].reset_index(drop=True)


# ---------- clean → curated ----------

def outages(times: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Intervalles sans aucun changement dans tout le réseau pendant plus de OUTAGE."""
    t = pd.concat([pd.Series([start, end]), times]).drop_duplicates().sort_values().reset_index(drop=True)
    gaps = t.diff() > OUTAGE
    return list(zip(t.shift()[gaps], t[gaps]))


def station_hourly(events: pd.DataFrame, carry: pd.DataFrame | None,
                   month_start: pd.Timestamp, month_end: pd.Timestamp) -> pd.DataFrame:
    """events : changements du mois (sortie de to_clean). carry : dernier état de chaque station au mois précédent."""
    if events.empty:
        return pd.DataFrame(columns=STATION_HOURLY_SCHEMA.names)
    end = min(month_end, events["timestamp"].max().ceil("h"))       # fichier incomplet : on ne prolonge pas
    ev = events.assign(is_event=True)
    if carry is not None and not carry.empty:                       # état au début du mois
        ev = pd.concat([carry.assign(timestamp=month_start, is_event=False), ev], ignore_index=True)
    ev = ev.sort_values(["station_id", "timestamp"]).reset_index(drop=True)
    gaps = outages(events["timestamp"], month_start if carry is not None else events["timestamp"].min(), end)

    # usage : |Δ vélos| entre deux changements successifs, sauf si une panne de la source les sépare
    g = ev.groupby("station_id", sort=False)
    prev_b, prev_t = g["bikes_available"].shift(), g["timestamp"].shift()
    usage = (ev["bikes_available"] - prev_b).abs()
    if gaps:
        ns = lambda s: s.dt.as_unit("ns").astype("int64").to_numpy()  # noqa: E731
        ends = ns(pd.Series([e for _, e in gaps]))
        crossed = (np.searchsorted(ends, ns(prev_t.fillna(month_start)), "right")
                   != np.searchsorted(ends, ns(ev["timestamp"]), "right"))
        usage = usage.mask(crossed)
    ev["usage"] = usage.where(ev["is_event"]).fillna(0).astype(int)

    # segments : chaque état dure jusqu'au changement suivant, puis découpage par heure
    ev["seg_end"] = g["timestamp"].shift(-1).fillna(end)
    seg = ev[ev["seg_end"] > ev["timestamp"]].copy()
    h0, h1 = seg["timestamp"].dt.floor("h"), (seg["seg_end"] - pd.Timedelta(1, "us")).dt.floor("h")
    n = ((h1 - h0) // HOUR).astype(int) + 1
    rep = seg.loc[seg.index.repeat(n)].copy()
    rep["hour"] = h0.loc[rep.index] + rep.groupby(level=0).cumcount() * HOUR
    first = ~rep.index.duplicated()                                 # seul le 1er morceau porte l'événement
    rep["usage"] = rep["usage"].where(first, 0)
    rep["is_event"] = rep["is_event"] & first
    w = (np.minimum(rep["seg_end"], rep["hour"] + HOUR) - np.maximum(rep["timestamp"], rep["hour"])).dt.total_seconds()
    rep["w"] = w
    for c in ("bikes_available", "free_slots", "capacity", "occupancy_rate"):
        v = rep[c].astype(float)
        rep[f"{c}_vw"], rep[f"{c}_w"] = (v * w).fillna(0), w.where(v.notna(), 0)

    agg = rep.groupby(["station_id", "hour"], sort=False).agg(
        station_name=("station_name", "last"), network_id=("network_id", "last"), city=("city", "last"),
        latitude=("latitude", "last"), longitude=("longitude", "last"),
        usage_events=("usage", "sum"), nb_readings=("is_event", "sum"),
        **{f"{c}_{s}": (f"{c}_{s}", "sum") for c in ("bikes_available", "free_slots", "capacity", "occupancy_rate")
           for s in ("vw", "w")})
    for c, nd in (("bikes_available", 2), ("free_slots", 2), ("capacity", 2), ("occupancy_rate", 4)):
        agg[c] = (agg[f"{c}_vw"] / agg[f"{c}_w"].where(agg[f"{c}_w"] > 0)).round(nd)
    out = agg.reset_index().rename(columns={"hour": "timestamp"})

    # heures touchées par une panne de la source (≥ 30 min) : retirées
    bad = set()
    for s, e in gaps:
        h = s.floor("h")
        while h < e:
            if min(e, h + HOUR) - max(s, h) >= pd.Timedelta(minutes=30):
                bad.add(h)
            h += HOUR
    out = out[~out["timestamp"].isin(bad) & out["city"].notna()]
    out["is_empty"] = (out["occupancy_rate"] < EMPTY).where(out["occupancy_rate"].notna())
    out["is_full"] = (out["occupancy_rate"] > FULL).where(out["occupancy_rate"].notna())
    return out[STATION_HOURLY_SCHEMA.names].reset_index(drop=True)


def city_hourly(st: pd.DataFrame) -> pd.DataFrame:
    """Même calcul que curate.city_hourly (Spark)."""
    agg = st.groupby(["city", "timestamp"]).agg(
        bikes_available=("bikes_available", "sum"), free_slots=("free_slots", "sum"),
        capacity=("capacity", "sum"), usage_events=("usage_events", "sum"),
        nb_stations=("station_id", "nunique"),
        nb_empty_stations=("is_empty", lambda s: s.fillna(False).astype(int).sum()),
        nb_full_stations=("is_full", lambda s: s.fillna(False).astype(int).sum())).reset_index()
    for c in ("bikes_available", "free_slots", "capacity"):
        agg[c] = agg[c].round(1)
    agg["occupancy_rate"] = (agg["bikes_available"] / agg["capacity"].where(agg["capacity"] > 0)).round(4)
    for c in ("bikes_available", "free_slots", "capacity"):
        agg[c] = agg[c].round()
    return agg[CITY_HOURLY_SCHEMA.names]


# ---------- écriture ----------

def _write(df: pd.DataFrame, schema: pa.Schema, layer: str, dataset: str, year: int, month: int, name: str) -> None:
    d = config.LOCAL_LAKE / layer / dataset / f"year={year}" / f"month={month}"
    d.mkdir(parents=True, exist_ok=True)
    for old in d.glob(f"{name}*"):
        old.unlink()
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), d / f"{name}.parquet")


def _drop_history(layer: str, dataset: str, year: int, month: int, name: str) -> None:
    d = config.LOCAL_LAKE / layer / dataset / f"year={year}" / f"month={month}"
    for old in d.glob(f"{name}*"):
        old.unlink()


def live_months() -> set[tuple[int, int]]:
    base = config.LOCAL_LAKE / "clean" / LIVE_CLEAN
    return {(int(y.name[5:]), int(m.name[6:])) for y in base.glob("year=*") for m in y.glob("month=*")}


def process() -> None:
    raw_dir = config.LOCAL_LAKE / "raw" / RAW
    live = live_months()
    now = pd.Timestamp(datetime.now(timezone.utc)).floor("s")
    files = sorted(raw_dir.glob("year=*/month=*/*-stats.parquet"), key=lambda p: p.name)
    by_month: dict[tuple[int, int], list] = {}
    carry: dict[str, pd.DataFrame] = {}
    prev: dict[str, tuple[int, int]] = {}
    for path in files:
        year, month, network = int(path.name[:4]), int(path.name[4:6]), path.name[7:-len("-stats.parquet")]
        if (year, month) in live:
            print(f"{path.name} : mois couvert par le collecteur, ignoré")
            continue
        raw = pq.read_table(path, columns=["id", "name", "latitude", "longitude", "bikes", "free", "timestamp"])
        events = to_clean(raw.to_pandas(), network, now)
        start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
        end = start + pd.offsets.MonthBegin(1)
        # report de l'état seulement si le mois précédent de ce réseau existe (sinon on ne sait pas)
        prev_month = (year - (month == 1), 12 if month == 1 else month - 1)
        c = carry.get(network) if prev.get(network) == prev_month else None
        st = station_hourly(events, c, start, end)
        carry[network] = events.groupby("station_id").tail(1)
        prev[network] = (year, month)
        _write(events, EVENTS_SCHEMA, "clean", CLEAN_EVENTS, year, month, f"history-{network}")
        by_month.setdefault((year, month), []).append(st)
        print(f"{path.name} : {len(events):,} changements → {len(st):,} lignes station × heure")

    for (year, month), parts in sorted(by_month.items()):
        st = pd.concat(parts, ignore_index=True)
        # partitions écrites par l'historique seulement (curate.py écrit les mois du collecteur, en part-*)
        _drop_history("curated", STATION_HOURLY, year, month, "part-")
        _drop_history("curated", CITY_HOURLY, year, month, "part-")
        _write(st, STATION_HOURLY_SCHEMA, "curated", STATION_HOURLY, year, month, "history")
        _write(city_hourly(st), CITY_HOURLY_SCHEMA, "curated", CITY_HOURLY, year, month, "history")
    print(f"curated : {len(by_month)} mois d'historique écrits ({config.LOCAL_LAKE / 'curated' / 'citybikes'})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-download", action="store_true", help="ne traiter que les fichiers déjà dans raw")
    a = ap.parse_args()
    if not a.no_download:
        download()
    process()
