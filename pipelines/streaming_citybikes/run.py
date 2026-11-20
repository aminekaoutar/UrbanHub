"""Développeur 2 · enchaîne clean (aujourd'hui + hier) → curated → publish PostgreSQL.

    python -m pipelines.streaming_citybikes.run              # une fois
    python -m pipelines.streaming_citybikes.run --every 3600 # toutes les heures (fréquence décidée au lancement)
"""
import argparse
import time
from datetime import datetime, timedelta, timezone

from pipelines.streaming_citybikes import clean, curate


def once():
    today = datetime.now(timezone.utc).date()
    for day in (today - timedelta(days=1), today):          # hier aussi : les relevés de 23 h-minuit arrivent après minuit
        try:
            clean.run(day.isoformat())
        except Exception as e:  # noqa: BLE001 - jour sans données (ex. premier jour)
            print(f"clean {day} ignoré : {type(e).__name__}")
    curate.run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, help="relance toutes les N secondes")
    a = ap.parse_args()
    while True:
        once()
        if not a.every:
            break
        time.sleep(a.every)
