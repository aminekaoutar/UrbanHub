"""Script unique qui enchaîne les traitements de toutes les sources (guide, étape 3).

À chaque passage, dans l'ordre :
    météo       : openmeteo (météo récente) -> curate (weather_city_hourly)
    pollution   : clean -> curate (pollution_city_hourly)
    vélos       : run (clean -> curate -> mobility_city_hourly)      si le code de Dev 2 est présent
    croisé      : urban (urban_city_hourly)                          si le code de Dev 3 est présent
Chaque table finale est publiée dans PostgreSQL : le dashboard Superset se met à jour.

Chaque étape tourne dans son propre processus : si l'une échoue (API indisponible...),
les suivantes sont quand même lancées, et la prochaine tentative a lieu au passage suivant.
La collecte en continu (vélos toutes les 3 min, pollution toutes les 15 min) est faite à part,
par les collecteurs (services "collecte-*" de infra/docker-compose.yml).

Lancement (dans le conteneur jupyter, depuis la racine du projet) :
    python3 -m pipelines.run_pipeline              # un seul passage
    python3 -m pipelines.run_pipeline --every 3600 # un passage toutes les heures
En continu, c'est le service "pipeline" de infra/docker-compose.yml qui le lance.
"""
from __future__ import annotations

import argparse
import importlib.util
import logging
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG_FILE = ROOT / "logs" / "run_pipeline.log"

# (nom affiché, module Python lancé avec "python3 -m")
STEPS = [
    ("météo · Open-Meteo", "pipelines.batch_noaa.openmeteo"),
    ("météo · curate", "pipelines.batch_noaa.curate"),
    ("pollution · clean", "pipelines.iot_openaq.clean"),
    ("pollution · curate", "pipelines.iot_openaq.curate"),
    ("vélos · clean + curate", "pipelines.streaming_citybikes.run"),
    ("croisé · urban_city_hourly", "pipelines.iot_openaq.urban"),
]

log = logging.getLogger("run_pipeline")


def setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    for handler in (RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3),
                    logging.StreamHandler()):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    log.setLevel(logging.INFO)


def run_once() -> bool:
    """Un passage complet. Renvoie True si toutes les étapes présentes ont réussi."""
    start, ok = time.monotonic(), True
    for name, module in STEPS:
        if importlib.util.find_spec(module) is None:
            log.info("%-28s : sauté (code pas encore dans le projet)", name)
            continue
        t0 = time.monotonic()
        result = subprocess.run([sys.executable, "-m", module], cwd=ROOT,
                                capture_output=True, text=True)
        seconds = time.monotonic() - t0
        if result.returncode == 0:
            log.info("%-28s : OK (%.0f s)", name, seconds)
        else:
            ok = False
            last_lines = "\n    ".join((result.stderr or result.stdout).strip().splitlines()[-6:])
            log.error("%-28s : ÉCHEC (%.0f s)\n    %s", name, seconds, last_lines)
    log.info("passage terminé en %.1f min%s", (time.monotonic() - start) / 60,
             "" if ok else " (avec des erreurs, voir ci-dessus)")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--every", type=int, help="relancer toutes les N secondes (ex. 3600)")
    args = parser.parse_args()
    setup_logging()

    while True:
        ok = run_once()
        if not args.every:
            sys.exit(0 if ok else 1)
        log.info("prochain passage dans %d min", args.every // 60)
        time.sleep(args.every)


if __name__ == "__main__":
    main()
