"""Développeur 2 · Collecteur CityBikes (flux streaming).

Toutes les 3 minutes, 24 h/24, interroge les réseaux de vélos de 9 des 10 villes du référentiel (Nice : aucun réseau à stations) et dépose chaque
réponse de l'API telle quelle (JSON compressé gzip) dans :

    raw/citybikes/networks/year=YYYY/month=MM/day=DD/<network>_<YYYYMMDDTHHMMSSZ>.json.gz

L'API ne donne pas d'historique : l'historique, c'est cette collecte. Elle doit tourner en continu.

Quota CityBikes : 300 appels / heure. 9 réseaux toutes les 3 min = 180 appels / heure (marge pour les reprises).

Lancement :
    python -m pipelines.streaming_citybikes.collector            # boucle infinie
    python -m pipelines.streaming_citybikes.collector --once     # un seul passage (test)
"""
import argparse
import gzip
import json
import logging
import random
import signal
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import requests

from common import config, storage

# Réseau CityBikes de chaque ville du référentiel (contrat de données, section 4.1)
NETWORKS = {
    "paris": "velib",
    "lyon": "velov",
    "marseille": "le-velo",
    "toulouse": "velo",
    # "nice": "pony-nice",   # 0 station dans CityBikes (Pony = flotte libre, sans station) : Nice non couverte
    "nantes": "bicloo",
    "strasbourg": "velhop",
    "montpellier": "velomagg",
    "bordeaux": "v3-bordeaux",
    "lille": "vlille",
}

LOG_FILE = config.ROOT / "logs" / "citybikes_collector.log"
log = logging.getLogger("citybikes")
_stop = False


def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    for h in (RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5), logging.StreamHandler()):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def fetch(session: requests.Session, network: str, attempts: int = 4) -> bytes:
    """Appel /v2/networks/<id> avec reprises (erreurs réseau, 5xx, 429 avec Retry-After)."""
    url = f"{config.CITYBIKES_BASE_URL}/networks/{network}"
    for i in range(1, attempts + 1):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning("%s : quota atteint (429), pause %s s", network, wait)
                time.sleep(min(wait, 120))
                continue
            r.raise_for_status()
            remaining = r.headers.get("x-ratelimit-remaining-hour")
            if remaining is not None and int(remaining) < 20:
                log.warning("quota CityBikes presque épuisé : %s appels restants cette heure", remaining)
            return r.content
        except requests.RequestException as e:
            if i == attempts:
                raise
            wait = 2 ** i + random.random()
            log.warning("%s : tentative %d/%d échouée (%s), nouvel essai dans %.0f s", network, i, attempts, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"{network} : abandon après {attempts} tentatives")


def raw_key(network: str, at: datetime) -> str:
    return (f"citybikes/networks/year={at:%Y}/month={at:%m}/day={at:%d}/"
            f"{network}_{at:%Y%m%dT%H%M%SZ}.json.gz")


def collect_once(session: requests.Session) -> dict:
    """Un passage sur les 10 réseaux. Renvoie {réseau: nb de stations} (ou l'erreur)."""
    result = {}
    for city, network in NETWORKS.items():
        at = datetime.now(timezone.utc)
        try:
            body = fetch(session, network)
            n = len(json.loads(body)["network"]["stations"])
            storage.put_bytes("raw", raw_key(network, at), gzip.compress(body), "application/gzip")
            result[network] = n
        except Exception as e:  # noqa: BLE001 - un réseau en panne ne doit pas arrêter les autres
            result[network] = f"ERREUR {e}"
            log.error("%s (%s) : %s", network, city, e)
    return result


def run(interval: int, once: bool = False):
    session = requests.Session()
    session.headers["User-Agent"] = "UrbanHub-student-project/1.0"
    log.info("Collecteur CityBikes démarré : %d réseaux, toutes les %d s, sortie %s",
             len(NETWORKS), interval, "MinIO + local" if config.MINIO_ENABLED else config.LOCAL_LAKE / "raw")
    while not _stop:
        start = time.monotonic()
        res = collect_once(session)
        ok = sum(isinstance(v, int) for v in res.values())
        log.info("passage terminé : %d/%d réseaux OK, %d stations (%.1f s)", ok, len(res),
                 sum(v for v in res.values() if isinstance(v, int)), time.monotonic() - start)
        if once:
            break
        # attente jusqu'au prochain créneau (le temps d'appel est déduit de l'intervalle)
        remaining = interval - (time.monotonic() - start)
        while remaining > 0 and not _stop:
            time.sleep(min(remaining, 5))
            remaining -= 5
    log.info("Collecteur arrêté")


def _handle_stop(*_):
    global _stop
    _stop = True


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="un seul passage puis arrêt")
    ap.add_argument("--interval", type=int, default=config.CITYBIKES_INTERVAL_SECONDS)
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    setup_logging()
    run(args.interval, args.once)
