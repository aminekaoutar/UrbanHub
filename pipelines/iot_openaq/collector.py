"""Développeur 3 · Collecteur OpenAQ v3 (pollution, capteurs IoT).

Trois commandes :

    python -m pipelines.iot_openaq.collector discover   # liste les capteurs de chaque ville
    python -m pipelines.iot_openaq.collector backfill   # historique 2024 → aujourd'hui
    python -m pipelines.iot_openaq.collector live       # boucle : dernières heures, toutes les 15 min
    python -m pipelines.iot_openaq.collector live --once

Ce qui est écrit dans la couche raw (JSON de l'API tel quel, compressé gzip) :

    raw/openaq/locations/year=YYYY/month=MM/day=DD/<city>_<YYYYMMDDTHHMMSSZ>.json.gz
        → liste des stations d'une ville (position, capteurs, polluant mesuré, unité)
    raw/openaq/sensor_hours/year=YYYY/month=MM/day=DD/sensor_<id>_<from>_<to>.json.gz
        → moyennes horaires d'un capteur ; la date du chemin est le début de la période
          (backfill : un fichier par mois, day=01 ; live : un fichier par jour, réécrit à chaque passage)

L'étape clean fusionne les deux (sensor_id → position) et supprime les doublons (sensor_id, timestamp).

Quota OpenAQ : 60 appels / minute et 2 000 / heure. On fait au plus 1 appel par seconde.
"""
import argparse
import gzip
import json
import logging
import signal
import time
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

import requests

from common import config, storage
from common.cities import CITIES, START_YEAR, assign_city
from common.schemas import POLLUTANTS

BASE_URL = "https://api.openaq.org/v3"
SEARCH_RADIUS_M = 25_000  # maximum autorisé par l'API ; assign_city vérifie ensuite les 30 km
MIN_SECONDS_BETWEEN_CALLS = 1.1
LIVE_INTERVAL_SECONDS = 15 * 60
LATE_HOURS = 3  # avant 3 h UTC, le live relit aussi la veille (les mesures arrivent en retard)

LOG_FILE = config.ROOT / "logs" / "openaq_collector.log"
log = logging.getLogger("openaq")
_stop = False


# --- Appels API -------------------------------------------------------------

class OpenAQClient:
    """Petit client HTTP : clé API, 1 appel/s max, reprises sur erreurs et quota (429)."""

    def __init__(self, api_key: str):
        if not api_key:
            raise SystemExit("OPENAQ_API_KEY manquante : ajoute ta clé dans le fichier .env")
        self.session = requests.Session()
        self.session.headers["X-API-Key"] = api_key
        self._last_call = 0.0

    def get(self, path: str, params: dict | None = None, attempts: int = 4) -> dict:
        for i in range(1, attempts + 1):
            wait = MIN_SECONDS_BETWEEN_CALLS - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()
            try:
                r = self.session.get(f"{BASE_URL}{path}", params=params, timeout=60)
                if r.status_code == 429:
                    pause = int(float(r.headers.get("x-ratelimit-reset", 60))) + 1
                    log.warning("quota OpenAQ atteint (429), pause %s s", pause)
                    time.sleep(min(pause, 300))
                    continue
                if r.status_code in (401, 403):
                    raise SystemExit("clé OpenAQ refusée (401/403) : vérifie OPENAQ_API_KEY dans .env")
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                if i == attempts:
                    raise
                log.warning("%s : tentative %d/%d échouée (%s)", path, i, attempts, e)
                time.sleep(2**i)
        raise RuntimeError(f"{path} : abandon après {attempts} tentatives")

    def get_all_pages(self, path: str, params: dict) -> dict:
        """Récupère toutes les pages (1 000 résultats max par page) dans une seule réponse."""
        params = {**params, "limit": 1000, "page": 1}
        results, first = [], None
        while True:
            data = self.get(path, params)
            first = first or data
            results += data.get("results", [])
            if len(data.get("results", [])) < params["limit"]:
                break
            params["page"] += 1
        return {**first, "results": results}


# --- Fonctions pures (testées sans réseau) ----------------------------------

def utc_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def partition(d: date) -> str:
    return f"year={d.year:04d}/month={d.month:02d}/day={d.day:02d}"


def locations_key(city: str, now: datetime) -> str:
    return f"openaq/locations/{partition(now.date())}/{city}_{utc_stamp(now)}.json.gz"


def sensor_hours_key(sensor_id: int, start: datetime, end: datetime) -> str:
    return (
        f"openaq/sensor_hours/{partition(start.date())}/"
        f"sensor_{sensor_id}_{utc_stamp(start)}_{utc_stamp(end)}.json.gz"
    )


def select_sensors(locations: list[dict], city: str, since: datetime) -> list[dict]:
    """Garde les capteurs utiles : station officielle et fixe, dans la ville, active depuis `since`,
    polluant du contrat.

    Les capteurs bon marché (AirGradient, HabitatMap...) ont isMonitor = False : on les écarte,
    car ils sont moins fiables que les stations de référence (décision du 2026-11-19).
    """
    selected = []
    for loc in locations:
        if not loc.get("isMonitor") or loc.get("isMobile"):
            continue
        coords = loc.get("coordinates") or {}
        if assign_city(coords.get("latitude"), coords.get("longitude")) != city:
            continue
        last = (loc.get("datetimeLast") or {}).get("utc")
        if last and datetime.fromisoformat(last.replace("Z", "+00:00")) < since:
            continue
        for sensor in loc.get("sensors", []):
            param = sensor.get("parameter") or {}
            if param.get("name") in POLLUTANTS:
                selected.append(
                    {
                        "sensor_id": sensor["id"],
                        "location_id": loc["id"],
                        "location_name": loc.get("name"),
                        "pollutant": param["name"],
                        "unit": param.get("units"),
                    }
                )
    return selected


def month_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Découpe [start, end[ en mois calendaires : [(1er janv, 1er févr), ...]."""
    chunks, cur = [], start
    while cur < end:
        nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0)
        chunks.append((cur, min(nxt, end)))
        cur = nxt
    return chunks


# --- Étapes de collecte -----------------------------------------------------

def save_json(key: str, data: dict) -> str:
    return storage.put_bytes("raw", key, gzip.compress(json.dumps(data).encode()), "application/gzip")


def discover(client: OpenAQClient, since: datetime) -> dict[str, list[dict]]:
    """Pour chaque ville : télécharge la liste des stations (sauvée en raw) et renvoie les capteurs utiles."""
    now = datetime.now(timezone.utc)
    sensors = {}
    for city, (lat, lon) in CITIES.items():
        data = client.get_all_pages(
            "/locations", {"coordinates": f"{lat},{lon}", "radius": SEARCH_RADIUS_M}
        )
        save_json(locations_key(city, now), data)
        sensors[city] = select_sensors(data["results"], city, since)
        log.info("%s : %d stations, %d capteurs retenus", city, len(data["results"]), len(sensors[city]))
    return sensors


def fetch_sensor_hours(client: OpenAQClient, sensor_id: int, start: datetime, end: datetime) -> str:
    data = client.get_all_pages(
        f"/sensors/{sensor_id}/hours",
        {"datetime_from": start.isoformat(), "datetime_to": end.isoformat()},
    )
    data["ingestion_time"] = datetime.now(timezone.utc).isoformat()
    return save_json(sensor_hours_key(sensor_id, start, end), data)


def backfill(client: OpenAQClient, since: datetime):
    """Historique depuis `since`, mois par mois. Les mois déjà présents en raw sont sautés (reprise possible)."""
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    first_of_month = now.replace(day=1, hour=0)
    done = set(storage.list_keys("raw", "openaq/sensor_hours/"))
    for city, sensors in discover(client, since).items():
        for s in sensors:
            # Le mois en cours n'est pas figé : il est couvert par le mode live.
            for start, end in month_chunks(since, first_of_month):
                key = sensor_hours_key(s["sensor_id"], start, end)
                if key in done:
                    continue
                if _stop:
                    return
                fetch_sensor_hours(client, s["sensor_id"], start, end)
                log.info("%s · capteur %s (%s) · %s", city, s["sensor_id"], s["pollutant"], start.strftime("%Y-%m"))


def live_once(client: OpenAQClient, sensors: dict[str, list[dict]]):
    """Relit les heures du jour en cours pour chaque capteur (fichier réécrit à chaque passage)."""
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    days = [today]
    if now.hour < LATE_HOURS:  # tôt le matin : les dernières heures de la veille arrivent encore
        days.insert(0, today - timedelta(days=1))
    for day in days:
        for s in (s for lst in sensors.values() for s in lst):
            if _stop:
                return
            fetch_sensor_hours(client, s["sensor_id"], day, day + timedelta(days=1))
    log.info("passage live terminé (%d capteurs)", sum(map(len, sensors.values())))


def live(client: OpenAQClient, since: datetime, once: bool):
    sensors, discovered_on = discover(client, since), date.today()
    while not _stop:
        t0 = time.monotonic()
        if date.today() != discovered_on:  # nouvelle liste de stations chaque jour
            sensors, discovered_on = discover(client, since), date.today()
        live_once(client, sensors)
        if once:
            break
        time.sleep(max(0, LIVE_INTERVAL_SECONDS - (time.monotonic() - t0)))


# --- Lancement --------------------------------------------------------------

def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    for h in (RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5), logging.StreamHandler()):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def _handle_stop(*_):
    global _stop
    _stop = True
    log.info("arrêt demandé, fin après l'appel en cours")


def main():
    ap = argparse.ArgumentParser(description="Collecteur OpenAQ v3 (Développeur 3)")
    ap.add_argument("mode", choices=["discover", "backfill", "live"])
    ap.add_argument("--once", action="store_true", help="live : un seul passage")
    args = ap.parse_args()

    setup_logging()
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    client = OpenAQClient(config.OPENAQ_API_KEY)
    since = datetime(START_YEAR, 1, 1, tzinfo=timezone.utc)

    if args.mode == "discover":
        for city, sensors in discover(client, since).items():
            print(f"\n{city} : {len(sensors)} capteurs")
            for s in sensors:
                print(f"  {s['sensor_id']:>9}  {s['pollutant']:<5} {s['unit'] or '':<7} {s['location_name']}")
    elif args.mode == "backfill":
        backfill(client, since)
    else:
        live(client, since, args.once)


if __name__ == "__main__":
    main()
