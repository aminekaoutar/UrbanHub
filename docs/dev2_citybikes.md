# Développeur 2 · Flux streaming CityBikes (Partie 2)

## Ce qui est livré

| Fichier | Rôle |
|---|---|
| `pipelines/streaming_citybikes/collector.py` | Collecteur 24 h/24 : réseaux de paris, lyon, lille (`common/cities.py`) toutes les 2 min → `raw/citybikes/networks/year=/month=/day=/<réseau>_<UTC>.json.gz` |
| `pipelines/streaming_citybikes/clean.py` | raw → `clean/citybikes/station_status` (Parquet, year/month/day) |
| `pipelines/streaming_citybikes/curate.py` | clean → `curated/citybikes/station_hourly` + **`curated/citybikes/mobility_city_hourly`** (livraison contrat 4.3) + publication PostgreSQL |
| `pipelines/streaming_citybikes/run.py` | clean (hier + aujourd'hui) → curate → publish, une fois ou toutes les heures |
| `pipelines/streaming_citybikes/history.py` | **historique** data.citybik.es (nov. 2024 → sept. 2026) → `raw/citybikes/history`, `clean/citybikes/station_events`, mois passés de `station_hourly` et `mobility_city_hourly` |
| `pipelines/streaming_citybikes/city.py` | colonne `city` (règle du contrat : ville la plus proche < 30 km) ; utilise `common/cities.py` dès qu'il existe |
| `analysis/mobility/partie2_mobilite.ipynb` | les 5 questions métier + graphiques (`docs/resultats/figures/mobilite/`) |
| `common/config.py`, `spark.py`, `storage.py`, `publish.py` | code commun piloté par le Dev 2 (étape 1) |
| `tests/test_citybikes.py`, `tests/smoke_lake.py` | tests du pipeline et du contrat ; test Spark ↔ MinIO ↔ PostgreSQL (jalon 1) |

## ⚠️ La machine de collecte doit rester **branchée sur secteur**

Nuit du 18 au 19/11 : batterie à 1 % → veille forcée de 00:21 à 07:48 → ~7 h de données perdues. `caffeinate` empêche la veille normale, pas la veille batterie vide ni la fermeture du capot.

## Lancer

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt   # Java 11 ou 17 requis pour Spark
cp .env.example .env   # puis SPARK_MASTER_URL=local[*] tant que le cluster Spark n'est pas lancé

# collecteur en continu (Mac : caffeinate empêche la mise en veille)
nohup caffeinate -i .venv/bin/python -m pipelines.streaming_citybikes.collector > logs/collector_stdout.log 2>&1 &
tail -f logs/citybikes_collector.log

.venv/bin/python -m pipelines.streaming_citybikes.run        # clean → curated → PostgreSQL
.venv/bin/python -m pytest tests/test_citybikes.py
.venv/bin/python -m tests.smoke_lake                          # infra lancée + MINIO_ENABLED=true
```

Avant que l'infra soit prête, tout est écrit dans `data/` avec la même arborescence que MinIO. Ensuite :
`MINIO_ENABLED=true` dans `.env`, puis `python -m common.storage --layer raw --prefix citybikes/` pousse l'historique local dans MinIO.

## Décisions à valider (à reporter dans `docs/decisions.md`)

1. **2 min, villes du contrat uniquement (paris, lyon, lille).** Quota CityBikes = 300 appels/heure **par adresse IP** (partagé avec tout le wifi). 3 réseaux × 30/h = 90/h. Sur un 429, le collecteur attend le `Retry-After` au lieu de réessayer.
2. **Nice non couverte côté vélos** : le seul réseau CityBikes de Nice (`pony-nice`) renvoie 0 station (flotte libre). Le référentiel commun garde Nice pour la météo et la pollution.
3. **Marseille** (`le-velo`) : le flux renvoie actuellement `free_bikes = 0` dans les 229 stations (vérifié dans le JSON brut). Les données sont collectées, mais Marseille est exclue des analyses tant que la source est fausse.
4. **raw en `.json.gz`** : la réponse est gardée telle quelle, compressée (~130 Mo/jour au lieu de ~1,3 Go). Spark lit le gzip sans configuration.
5. **`timestamp`** = heure du relevé fournie par CityBikes (UTC) ; `ingestion_time` = heure de dépôt (nom du fichier).
6. **`usage_events`** = somme des |Δ bikes_available| entre deux relevés d'une station, **sauf si l'écart dépasse 15 min** (collecteur arrêté) ; c'est une borne basse de l'usage réel.
7. **Station vide / pleine** = taux de remplissage moyen sur l'heure < 10 % / > 90 % (contrat 4.3).
8. **Historique** : https://data.citybik.es (même source, mêmes `station_id`, licence ODbL, citer « Bike-share data by CityBikes contributors and available from https://data.citybik.es »). Le fichier ne contient que les changements d'état : état horaire = dernier état connu (moyenne pondérée par la durée), `usage_events` = somme exacte des |Δ vélos| (donc plus élevé que la borne basse du collecteur). Heures sans aucun changement dans tout le réseau pendant plus de 2 h = panne de la source → retirées. Mois non publiés : velib avr.–juil. 2026, velov juil.–août 2026, vlille juin–juil. 2026, oct. 2026 pour les trois. `python -m pipelines.streaming_citybikes.history` (~20 min, 1,1 Go).

## Points corrigés dans le code commun (à signaler en relecture)

- `get_spark()` force `PYSPARK_PYTHON` = Python du projet (sinon erreur `PYTHON_VERSION_MISMATCH` si le Python système est différent).
- `get_spark()` met la JVM en UTC : sans cela, l'écriture JDBC décalait les heures de +1 h dans PostgreSQL.
