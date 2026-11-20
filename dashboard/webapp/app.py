"""UrbanHub web app: a FastAPI server that answers every chart with a live SQL query.

The database is the Data Lake's curated layer, read with DuckDB straight from the Parquet files
(`data/` locally). With WEBAPP_BACKEND=postgres, the curated tables are read from the PostgreSQL
serving database instead (the tables published by common/publish.py), the rest from the lake.

    .venv/bin/pip install -r dashboard/webapp/requirements.txt
    .venv/bin/python -m dashboard.webapp            # then open http://127.0.0.1:8000
"""
import gzip
import json
import os
import re
import time
from datetime import date
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[2]
STATIC = Path(__file__).resolve().parent / "static"
LAKE = Path(os.getenv("LOCAL_LAKE", ROOT / "data"))
BACKEND = os.getenv("WEBAPP_BACKEND", "lake").lower()
CITIES = ("paris", "lyon", "lille")
LOCAL = "timezone('Europe/Paris', timestamp)"          # UTC → Paris time, for days and hours
WHO_DAY = {"pm25": 15, "pm10": 45, "no2": 25, "o3": 100, "co": 4000}
WHO_YEAR = {"pm25": 5, "pm10": 15, "no2": 10}
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co")

# table name → (lake path relative to data/, PostgreSQL table name or None, description, time column)
TABLES = {
    "weather_city_hourly": (None, "weather_city_hourly", "Weather per city and hour (Dev 1: NOAA, then Open-Meteo)", "timestamp"),
    "mobility_city_hourly": ("curated/citybikes/mobility_city_hourly", "mobility_city_hourly", "Bike-share per city and hour (Dev 2)", "timestamp"),
    "station_hourly": ("curated/citybikes/station_hourly", "station_hourly", "Bike-share per station and hour (Dev 2)", "timestamp"),
    "vlille_trips": ("clean/ilevia/vlille_trips", None, "Every V'Lille trip since April 2025 (Dev 2, ilévia open data)", "start_time"),
    "pollution_city_hourly": ("curated/openaq/pollution_city_hourly", "pollution_city_hourly", "Air quality per city and hour (Dev 3)", "timestamp"),
    "measurements": ("clean/openaq/measurements", None, "Air-quality measurements per sensor and hour (Dev 3, clean layer)", "timestamp"),
}
AVAILABLE: set[str] = set()


def _parquet(rel: str, ts: str = "timestamp") -> str:
    return (f"SELECT * REPLACE (CAST({ts} AS TIMESTAMPTZ) AS {ts}) "
            f"FROM read_parquet('{(LAKE / rel).as_posix()}/**/*.parquet', union_by_name=true, hive_partitioning=true)")


def _weather_source() -> str:
    # Dev 1's official table if it is in the lake, else the local Open-Meteo copy
    for rel in ("curated/noaa/weather_city_hourly", "curated/openmeteo/weather_city_hourly"):
        if any((LAKE / rel).glob("**/*.parquet")):
            return rel
    raise RuntimeError("No weather table found in the Data Lake")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    pg = None
    if BACKEND == "postgres":
        dsn = (f"host={os.getenv('POSTGRES_HOST', 'localhost')} port={os.getenv('POSTGRES_PORT', '5432')} "
               f"dbname={os.getenv('POSTGRES_DB', 'urbanhub')} user={os.getenv('POSTGRES_USER', 'urbanhub')} "
               f"password={os.getenv('POSTGRES_PASSWORD', 'urbanhub123')}")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY)")
        pg = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'pg'").fetchall()}
    for name, (rel, pg_name, _, ts) in TABLES.items():
        if pg and pg_name in pg:
            con.execute(f"CREATE VIEW {name} AS SELECT * REPLACE (CAST({ts} AS TIMESTAMPTZ) AS {ts}) FROM pg.public.{pg_name}")
        else:
            rel = rel or _weather_source()
            if not any((LAKE / rel).glob("**/*.parquet")):
                continue                                      # optional table not downloaded yet
            con.execute(f"CREATE VIEW {name} AS {_parquet(rel, ts)}")
        AVAILABLE.add(name)
    # OpenAQ station names, from the raw "locations" answers
    names = {}
    for f in (LAKE / "raw/openaq/locations").glob("**/*.json.gz"):
        for loc in json.load(gzip.open(f)).get("results", []):
            for s in loc.get("sensors", []):
                names[int(s["id"])] = loc.get("name") or f"Sensor {s['id']}"
    con.execute("CREATE TABLE sensor_names (sensor_id BIGINT, name VARCHAR)")
    if names:
        con.executemany("INSERT INTO sensor_names VALUES (?, ?)", list(names.items()))
    return con


CON = connect()
app = FastAPI(title="UrbanHub API", version="1.0")


# ---------------- helpers ----------------

def run(sql: str, params: list | None = None) -> dict:
    t = time.perf_counter()
    cur = CON.cursor()
    try:
        res = cur.execute(sql, params or [])
        cols = [d[0] for d in res.description]
        rows = [dict(zip(cols, r)) for r in res.fetchall()]
    finally:
        cur.close()
    for r in rows:                                      # JSON-friendly numbers and dates
        for k, v in r.items():
            if isinstance(v, float):
                r[k] = None if v != v else round(v, 3)
            elif isinstance(v, date):
                r[k] = v.isoformat()
    shown = sql
    for p in params or []:
        shown = shown.replace("?", repr(p) if isinstance(p, str) else str(p), 1)
    return {"rows": rows, "sql": re.sub(r"\n\s+", "\n  ", shown.strip()), "ms": round((time.perf_counter() - t) * 1000, 1)}


DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def period(start: str | None, end: str | None) -> tuple[str, str]:
    start, end = start or "2024-01-01", end or "2100-01-01"
    if not (DAY.match(start) and DAY.match(end)):
        raise HTTPException(400, "Dates must look like 2025-06-30")
    return start, end


def city_ok(city: str) -> str:
    if city not in CITIES:
        raise HTTPException(400, f"city must be one of {', '.join(CITIES)}")
    return city


def cities_ok(cities: str | None) -> list[str]:
    out = [c for c in (cities or ",".join(CITIES)).split(",") if c]
    for c in out:
        city_ok(c)
    return out


WHERE_T = f"CAST({LOCAL} AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)"


# ---------------- data catalogue ----------------

@app.get("/api/meta")
def meta():
    try:
        _trips()
    except HTTPException:
        pass
    tables = []
    for name, (rel, pg_name, desc, ts) in TABLES.items():
        if name not in AVAILABLE:
            continue
        lt = f"timezone('Europe/Paris', {ts})"
        r = run(f"SELECT count(*) AS n, strftime(min({lt}), '%Y-%m-%d') AS first, strftime(max({lt}), '%Y-%m-%d %H:00') AS last FROM {name}")["rows"][0]
        cols = run(f"DESCRIBE {name}")["rows"]
        tables.append({"name": name, "description": desc, "rows": r["n"], "first": r["first"], "last": r["last"],
                       "columns": [{"name": c["column_name"], "type": c["column_type"]} for c in cols]})
    return {"backend": BACKEND, "lake": str(LAKE), "weather_source": _weather_source(), "tables": tables, "who_day": WHO_DAY, "who_year": WHO_YEAR,
            "has_trips": "vlille_trips" in AVAILABLE}


@app.get("/api/table/{name}")
def preview(name: str, limit: int = Query(20, ge=1, le=200)):
    if name not in AVAILABLE:
        raise HTTPException(404, "Unknown table")
    ts = TABLES[name][3]
    return run(f"SELECT * REPLACE (strftime(timezone('Europe/Paris', {ts}), '%Y-%m-%d %H:%M') AS {ts}) FROM {name} ORDER BY {ts} DESC LIMIT {int(limit)}")


@app.get("/api/coverage")
def coverage():
    parts = []
    for src, table in (("Weather", "weather_city_hourly"), ("Bikes", "mobility_city_hourly"), ("Pollution", "pollution_city_hourly")):
        parts.append(f"SELECT '{src}' AS source, city, strftime({LOCAL}, '%Y-%m') AS month, count(DISTINCT CAST({LOCAL} AS DATE)) AS days "
                     f"FROM {table} WHERE city IN ('paris','lyon','lille') GROUP BY ALL")
    return run(" UNION ALL ".join(parts) + " ORDER BY source, city, month")


# ---------------- Part 1 · weather ----------------

def weather_daily_sql():
    return f"""SELECT city, CAST({LOCAL} AS DATE) AS day, avg(temperature) AS tmean, max(temperature) AS tmax,
                      min(temperature) AS tmin, sum(precipitation) AS rain, avg(wind_speed) AS wind, avg(visibility) AS visibility, count(*) AS hours
               FROM weather_city_hourly WHERE city IN ('paris','lyon','lille') AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 18"""


@app.get("/api/weather/daily")
def weather_daily(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"SELECT * FROM ({weather_daily_sql()}) WHERE city = ? ORDER BY day", [s, e, city_ok(city)])


@app.get("/api/weather/monthly")
def weather_monthly(start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT city, strftime(day, '%Y-%m') AS month, avg(tmean) AS tmean, max(tmax) AS tmax, min(tmin) AS tmin, sum(rain) AS rain
                   FROM ({weather_daily_sql()}) GROUP BY ALL ORDER BY city, month""", [s, e])


@app.get("/api/weather/anomalies")
def weather_anomalies(start: str | None = None, end: str | None = None, z: float = Query(1.5, ge=0.5, le=4), min_days: int = Query(3, ge=1, le=15)):
    """Gaps-and-islands: days more than z standard deviations from their city-month normal, in runs of min_days or more."""
    s, e = period(start, end)
    return run(f"""
        WITH d AS ({weather_daily_sql()}),
        n AS (SELECT *, (tmean - avg(tmean) OVER w) / nullif(stddev(tmean) OVER w, 0) AS z,
                     avg(tmean) OVER w AS normal
              FROM d WINDOW w AS (PARTITION BY city, month(day))),
        f AS (SELECT *, CASE WHEN z >= ? THEN 'Hot' WHEN z <= -? THEN 'Cold' END AS kind FROM n),
        g AS (SELECT *, day - CAST(row_number() OVER (PARTITION BY city, kind ORDER BY day) AS INTEGER) AS grp FROM f WHERE kind IS NOT NULL)
        SELECT city, kind AS type, min(day) AS start, max(day) AS "end", count(*) AS days, avg(tmean) AS tmean, avg(tmean - normal) AS gap
        FROM g GROUP BY city, kind, grp HAVING count(*) >= ? ORDER BY start""", [s, e, z, z, min_days])


@app.get("/api/weather/extremes")
def weather_extremes(start: str | None = None, end: str | None = None, heat: float = 33, cold: float = -5, rain: float = 20, wind: float = 12):
    s, e = period(start, end)
    return run(f"""
        WITH d AS (SELECT city, CAST({LOCAL} AS DATE) AS day, max(temperature) AS tmax, min(temperature) AS tmin,
                          sum(precipitation) AS rain, max(wind_speed) AS wind_max
                   FROM weather_city_hourly WHERE city IN ('paris','lyon','lille') AND {WHERE_T} GROUP BY ALL)
        SELECT city, day, type, tmax, tmin, rain, wind_max FROM (
            SELECT *, 'Heat' AS type FROM d WHERE tmax >= ? UNION ALL
            SELECT *, 'Severe cold' FROM d WHERE tmin <= ? UNION ALL
            SELECT *, 'Heavy rain' FROM d WHERE rain >= ? UNION ALL
            SELECT *, 'Strong wind' FROM d WHERE wind_max >= ?) ORDER BY day""", [s, e, heat, cold, rain, wind])


@app.get("/api/weather/visibility")
def weather_visibility(start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""
        WITH h AS (SELECT * FROM weather_city_hourly WHERE city IN ('paris','lyon','lille') AND visibility IS NOT NULL AND {WHERE_T})
        SELECT 'rain' AS by, CASE WHEN precipitation = 0 THEN '0 (dry)' WHEN precipitation < 1 THEN '0–1' WHEN precipitation < 2 THEN '1–2'
                                  WHEN precipitation < 5 THEN '2–5' ELSE '5 +' END AS band,
               avg(visibility) / 1000 AS visibility_km, avg(CASE WHEN visibility < 1000 THEN 100 ELSE 0 END) AS fog_pct, count(*) AS hours
        FROM h GROUP BY ALL
        UNION ALL
        SELECT 'wind', CASE WHEN wind_speed < 1 THEN '0–1' WHEN wind_speed < 2 THEN '1–2' WHEN wind_speed < 4 THEN '2–4'
                            WHEN wind_speed < 6 THEN '4–6' ELSE '6 +' END,
               avg(visibility) / 1000, avg(CASE WHEN visibility < 1000 THEN 100 ELSE 0 END), count(*)
        FROM h GROUP BY ALL ORDER BY 1, 2""", [s, e])


# ---------------- Part 2 · mobility ----------------

@app.get("/api/mobility/daily")
def mobility_daily(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT CAST({LOCAL} AS DATE) AS day, sum(usage_events) AS moves, avg(nb_stations) AS stations,
                          avg(100.0 * nb_empty_stations / nb_stations) AS empty_pct, avg(100.0 * nb_full_stations / nb_stations) AS full_pct
                   FROM mobility_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 20 ORDER BY day""", [city_ok(city), s, e])


@app.get("/api/mobility/profile")
def mobility_profile(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT CASE WHEN dayofweek({LOCAL}) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS daytype, hour({LOCAL}) AS hour,
                          avg(usage_events) AS moves, avg(100.0 * nb_empty_stations / nb_stations) AS empty_pct
                   FROM mobility_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL ORDER BY daytype, hour""", [city_ok(city), s, e])


@app.get("/api/mobility/dow")
def mobility_dow(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT isodow(day) AS dow, avg(moves) AS moves FROM (
                     SELECT CAST({LOCAL} AS DATE) AS day, sum(usage_events) AS moves FROM mobility_city_hourly
                     WHERE city = ? AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 20) GROUP BY ALL ORDER BY dow""", [city_ok(city), s, e])


@app.get("/api/mobility/stations")
def mobility_stations(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""
        SELECT station_id, last(station_name ORDER BY timestamp) AS name, last(latitude ORDER BY timestamp) AS lat, last(longitude ORDER BY timestamp) AS lon,
               avg(capacity) AS cap, 24.0 * sum(usage_events) / count(*) AS per_day,
               100 * avg(CAST(is_empty AS INTEGER)) AS empty_pct, 100 * avg(CAST(is_full AS INTEGER)) AS full_pct,
               100 * avg(occupancy_rate) FILTER (WHERE isodow({LOCAL}) <= 5 AND hour({LOCAL}) BETWEEN 1 AND 4) AS occ_night,
               100 * avg(occupancy_rate) FILTER (WHERE isodow({LOCAL}) <= 5 AND hour({LOCAL}) BETWEEN 11 AND 14) AS occ_day,
               count(*) AS hours
        FROM station_hourly WHERE city = ? AND {WHERE_T}
        GROUP BY station_id HAVING count(*) >= 24 * 7 AND sum(usage_events) > 0""", [city_ok(city), s, e])


@app.get("/api/mobility/station/{station_id}")
def mobility_station(station_id: str, start: str | None = None, end: str | None = None):
    if not re.match(r"^[0-9a-f]{8,64}$", station_id):
        raise HTTPException(400, "Unknown station id")
    s, e = period(start, end)
    prof = run(f"""SELECT CASE WHEN isodow({LOCAL}) >= 6 THEN 'Weekend' ELSE 'Weekday' END AS daytype, hour({LOCAL}) AS hour,
                          100 * avg(occupancy_rate) AS occ, avg(usage_events) AS moves
                   FROM station_hourly WHERE station_id = ? AND {WHERE_T} GROUP BY ALL ORDER BY daytype, hour""", [station_id, s, e])
    daily = run(f"""SELECT CAST({LOCAL} AS DATE) AS day, sum(usage_events) AS moves, 100 * avg(occupancy_rate) AS occ
                    FROM station_hourly WHERE station_id = ? AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 20 ORDER BY day""", [station_id, s, e])
    return {"profile": prof, "daily": daily}


@app.get("/api/mobility/heatmap")
def mobility_heatmap(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT isodow({LOCAL}) AS dow, hour({LOCAL}) AS hour, avg(usage_events) AS moves
                   FROM mobility_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL ORDER BY dow, hour""", [city_ok(city), s, e])


@app.get("/api/mobility/monthly")
def mobility_monthly(start: str | None = None, end: str | None = None):
    """Per city and month: movements per day, per station per day, network size and empty share."""
    s, e = period(start, end)
    return run(f"""
        WITH d AS (SELECT city, CAST({LOCAL} AS DATE) AS day, sum(usage_events) AS moves, avg(nb_stations) AS stations,
                          avg(100.0 * nb_empty_stations / nb_stations) AS empty_pct, avg(occupancy_rate) * 100 AS occ
                   FROM mobility_city_hourly WHERE city IN ('paris','lyon','lille') AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 20)
        SELECT city, strftime(day, '%Y-%m') AS month, avg(moves) AS moves, avg(moves / stations) AS moves_per_station,
               avg(stations) AS stations, avg(empty_pct) AS empty_pct, avg(occ) AS occ, count(*) AS days
        FROM d GROUP BY ALL ORDER BY city, month""", [s, e])


@app.get("/api/mobility/occupancy")
def mobility_occupancy(city: str, start: str | None = None, end: str | None = None):
    """How full stations are, hour by hour: share of station-hours in each 10 % band."""
    s, e = period(start, end)
    return run(f"""SELECT least(floor(occupancy_rate * 10), 9) * 10 AS band, 100.0 * count(*) / sum(count(*)) OVER () AS pct
                   FROM station_hourly WHERE city = ? AND occupancy_rate IS NOT NULL AND {WHERE_T} GROUP BY ALL ORDER BY band""", [city_ok(city), s, e])


def _trips():
    if "vlille_trips" not in AVAILABLE:                     # registered as soon as its files appear in the lake
        rel = TABLES["vlille_trips"][0]
        if not any((LAKE / rel).glob("**/*.parquet")):
            raise HTTPException(404, "The V'Lille trips table is not in the Data Lake yet")
        CON.execute(f"CREATE OR REPLACE VIEW vlille_trips AS {_parquet(rel, 'start_time')}")
        AVAILABLE.add("vlille_trips")


TRIP_T = "CAST(timezone('Europe/Paris', start_time) AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)"
TRIP_L = "timezone('Europe/Paris', start_time)"


@app.get("/api/trips/summary")
def trips_summary(start: str | None = None, end: str | None = None):
    _trips()
    s, e = period(start, end)
    return run(f"""SELECT count(*) AS trips, count(DISTINCT CAST({TRIP_L} AS DATE)) AS days, median(duration_min) AS median_min,
                          avg(duration_min) FILTER (WHERE duration_min <= 120) AS mean_min,
                          100.0 * avg(CASE WHEN start_station_id = end_station_id THEN 1 ELSE 0 END) AS round_trip_pct,
                          100.0 * avg(CASE WHEN duration_min <= 30 THEN 1 ELSE 0 END) AS under30_pct
                   FROM vlille_trips WHERE {TRIP_T}""", [s, e])


@app.get("/api/trips/daily")
def trips_daily(start: str | None = None, end: str | None = None):
    _trips()
    s, e = period(start, end)
    return run(f"""SELECT CAST({TRIP_L} AS DATE) AS day, count(*) AS trips, median(duration_min) AS median_min
                   FROM vlille_trips WHERE {TRIP_T} GROUP BY ALL ORDER BY day""", [s, e])


@app.get("/api/trips/durations")
def trips_durations(start: str | None = None, end: str | None = None):
    _trips()
    s, e = period(start, end)
    return run(f"""SELECT CASE WHEN duration_min < 60 THEN floor(duration_min / 5) * 5 ELSE 60 END AS bucket,
                          CASE WHEN isodow({TRIP_L}) >= 6 THEN 'Weekend' ELSE 'Weekday' END AS daytype, count(*) AS trips
                   FROM vlille_trips WHERE duration_min IS NOT NULL AND {TRIP_T} GROUP BY ALL ORDER BY bucket""", [s, e])


@app.get("/api/trips/hourly")
def trips_hourly(start: str | None = None, end: str | None = None):
    _trips()
    s, e = period(start, end)
    return run(f"""SELECT CASE WHEN isodow({TRIP_L}) >= 6 THEN 'Weekend' ELSE 'Weekday' END AS daytype, hour({TRIP_L}) AS hour,
                          count(*) / count(DISTINCT CAST({TRIP_L} AS DATE)) AS trips, median(duration_min) AS median_min
                   FROM vlille_trips WHERE {TRIP_T} GROUP BY ALL ORDER BY daytype, hour""", [s, e])


@app.get("/api/trips/flows")
def trips_flows(start: str | None = None, end: str | None = None, top: int = Query(40, ge=5, le=200)):
    _trips()
    s, e = period(start, end)
    return run(f"""SELECT start_station, end_station, any_value(start_lat) AS start_lat, any_value(start_lon) AS start_lon,
                          any_value(end_lat) AS end_lat, any_value(end_lon) AS end_lon, count(*) AS trips, median(duration_min) AS median_min
                   FROM vlille_trips WHERE start_station_id <> end_station_id AND start_lat IS NOT NULL AND end_lat IS NOT NULL AND {TRIP_T}
                   GROUP BY start_station, end_station ORDER BY trips DESC LIMIT {int(top)}""", [s, e])


@app.get("/api/trips/stations")
def trips_stations(start: str | None = None, end: str | None = None):
    """Departures and arrivals per station: stations that lose or gain bikes through trips."""
    _trips()
    s, e = period(start, end)
    return run(f"""
        WITH d AS (SELECT start_station AS station, any_value(start_lat) AS lat, any_value(start_lon) AS lon, count(*) AS departures
                   FROM vlille_trips WHERE {TRIP_T} GROUP BY 1),
             a AS (SELECT end_station AS station, count(*) AS arrivals FROM vlille_trips WHERE {TRIP_T} GROUP BY 1)
        SELECT station, lat, lon, departures, coalesce(arrivals, 0) AS arrivals, coalesce(arrivals, 0) - departures AS net
        FROM d LEFT JOIN a USING (station) WHERE lat IS NOT NULL ORDER BY departures DESC""", [s, e, s, e])


# ---------------- Part 3 · pollution ----------------

@app.get("/api/pollution/daily")
def pollution_daily(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT CAST({LOCAL} AS DATE) AS day, avg(pm25) AS pm25, avg(pm10) AS pm10, avg(no2) AS no2, avg(o3) AS o3, avg(co) AS co
                   FROM pollution_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL ORDER BY day""", [city_ok(city), s, e])


@app.get("/api/pollution/profile")
def pollution_profile(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT CASE WHEN isodow({LOCAL}) >= 6 THEN 'Weekend' ELSE 'Weekday' END AS daytype, hour({LOCAL}) AS hour,
                          avg(pm25) AS pm25, avg(pm10) AS pm10, avg(no2) AS no2, avg(o3) AS o3
                   FROM pollution_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL ORDER BY daytype, hour""", [city_ok(city), s, e])


@app.get("/api/pollution/monthly")
def pollution_monthly(city: str, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""SELECT strftime({LOCAL}, '%Y-%m') AS month, avg(pm25) AS pm25, avg(pm10) AS pm10, avg(no2) AS no2, avg(o3) AS o3
                   FROM pollution_city_hourly WHERE city = ? AND {WHERE_T} GROUP BY ALL ORDER BY month""", [city_ok(city), s, e])


@app.get("/api/pollution/stations")
def pollution_stations(start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    return run(f"""
        SELECT coalesce(n.name, 'Sensor ' || m.sensor_id) AS name, m.city, avg(m.latitude) AS lat, avg(m.longitude) AS lon,
               avg(value) FILTER (WHERE pollutant = 'pm25') AS pm25, avg(value) FILTER (WHERE pollutant = 'pm10') AS pm10,
               avg(value) FILTER (WHERE pollutant = 'no2') AS no2, count(*) AS measures
        FROM measurements m LEFT JOIN sensor_names n USING (sensor_id)
        WHERE m.city IN ('paris','lyon','lille') AND pollutant IN ('pm25','pm10','no2') AND {WHERE_T}
        GROUP BY ALL HAVING count(*) >= 24 * 7""", [s, e])


@app.get("/api/pollution/dominant")
def pollution_dominant(start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    ratios = ", ".join(f"avg({p}) / {WHO_DAY[p]} AS r_{p}" for p in POLLUTANTS)
    return run(f"""
        WITH d AS (SELECT city, CAST({LOCAL} AS DATE) AS day, {ratios}
                   FROM pollution_city_hourly WHERE city IN ('paris','lyon','lille') AND {WHERE_T} GROUP BY ALL),
        u AS (UNPIVOT d ON r_pm25, r_pm10, r_no2, r_o3, r_co INTO NAME pollutant VALUE ratio),
        top AS (SELECT city, day, arg_max(pollutant, ratio) AS dom FROM u GROUP BY ALL),
        c AS (SELECT city, dom, count(*) AS n FROM top GROUP BY city, dom)
        SELECT 'dominant' AS kind, city, replace(dom, 'r_', '') AS pollutant, 100.0 * n / sum(n) OVER (PARTITION BY city) AS pct
        FROM c
        UNION ALL
        SELECT 'above', city, replace(pollutant, 'r_', ''), 100.0 * avg(CASE WHEN ratio > 1 THEN 1 ELSE 0 END) FROM u GROUP BY ALL
        ORDER BY kind, city, pollutant""", [s, e])


@app.get("/api/pollution/episodes")
def pollution_episodes(city: str, pollutant: str = "pm25", start: str | None = None, end: str | None = None,
                       z: float = Query(3, ge=1, le=6), window: int = Query(30, ge=7, le=90)):
    if pollutant not in POLLUTANTS:
        raise HTTPException(400, "Unknown pollutant")
    s, e = period(start, end)
    return run(f"""
        WITH d AS (SELECT CAST({LOCAL} AS DATE) AS day, avg({pollutant}) AS value FROM pollution_city_hourly
                   WHERE city = ? AND {WHERE_T} GROUP BY ALL HAVING avg({pollutant}) IS NOT NULL)
        SELECT day, value, (value - avg(value) OVER w) / nullif(stddev(value) OVER w, 0) AS z,
               ((value - avg(value) OVER w) / nullif(stddev(value) OVER w, 0)) > ? AS episode
        FROM d WINDOW w AS (ORDER BY day ROWS BETWEEN {int(window)} PRECEDING AND 1 PRECEDING) ORDER BY day""", [city_ok(city), s, e, z])


# ---------------- Part 4 · cross ----------------

@app.get("/api/cross/daily")
def cross_daily(cities: str | None = None, start: str | None = None, end: str | None = None):
    s, e = period(start, end)
    cs = cities_ok(cities)
    marks = ",".join("?" for _ in cs)
    return run(f"""
        WITH b AS (SELECT city, CAST({LOCAL} AS DATE) AS day, sum(usage_events) AS moves FROM mobility_city_hourly
                   WHERE city IN ({marks}) AND {WHERE_T} GROUP BY ALL HAVING count(*) >= 20),
             w AS (SELECT city, CAST({LOCAL} AS DATE) AS day, avg(temperature) AS tmean, sum(precipitation) AS rain, avg(wind_speed) AS wind
                   FROM weather_city_hourly WHERE city IN ({marks}) AND {WHERE_T} GROUP BY ALL),
             p AS (SELECT city, CAST({LOCAL} AS DATE) AS day, avg(pm25) AS pm25, avg(no2) AS no2
                   FROM pollution_city_hourly WHERE city IN ({marks}) AND {WHERE_T} GROUP BY ALL)
        SELECT b.city, b.day, moves, tmean, rain, wind, pm25, no2, isodow(b.day) >= 6 AS weekend
        FROM b JOIN w USING (city, day) LEFT JOIN p USING (city, day) ORDER BY b.city, b.day""", [*cs, s, e, *cs, s, e, *cs, s, e])


# ---------------- front end ----------------

app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.exception_handler(duckdb.Error)
def db_error(request, exc):
    return JSONResponse({"detail": f"Database error: {exc}"}, status_code=500)
