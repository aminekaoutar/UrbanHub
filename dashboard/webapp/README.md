# UrbanHub · site web interactif

Un site qui interroge **en direct** la base de données du projet : chaque graphique est une requête SQL
exécutée au moment où on change la ville, la période ou un réglage.

```
navigateur  ──►  FastAPI (dashboard/webapp/app.py)  ──►  DuckDB  ──►  Data Lake (Parquet curated / clean)
                                                                └─►  ou PostgreSQL (WEBAPP_BACKEND=postgres)
```

## Lancer

```bash
.venv/bin/pip install -r dashboard/webapp/requirements.txt
.venv/bin/python -m dashboard.webapp           # puis ouvrir http://127.0.0.1:8000
```

| Variable | Défaut | Rôle |
|---|---|---|
| `LOCAL_LAKE` | `data/` | dossier du Data Lake local |
| `WEBAPP_BACKEND` | `lake` | `postgres` : lit les tables curated dans PostgreSQL (celles publiées par `common/publish.py`) |
| `POSTGRES_*` | `.env` | connexion PostgreSQL |
| `HOST`, `PORT` | `127.0.0.1`, `8000` | adresse du serveur |

Documentation de l'API (générée par FastAPI) : http://127.0.0.1:8000/docs

## Ce que montre le site

| Page | Contenu | Tables |
|---|---|---|
| Overview | parties du projet, chaîne de données, couverture mois par mois | toutes |
| 1 · Weather | périodes anormales (z-score réglable), visibilité, saisons, jours extrêmes (seuils réglables) | `weather_city_hourly` |
| 2 · Mobility | stations les plus utilisées, carte de l'offre, pics, heatmap semaine, déséquilibres nuit/midi, stations à rééquilibrer, comparaison des 3 réseaux, **trajets V'Lille** (flux, durées), fiche détaillée de chaque station | `station_hourly`, `mobility_city_hourly`, `vlille_trips` |
| 3 · Pollution | zones polluées vs seuils OMS, profil journalier, polluant dominant, épisodes (z-score et fenêtre réglables) | `pollution_city_hourly`, `measurements` |
| 4 · Cross analysis | pluie × vélos, température × vélos, vent × NO₂, explorateur et matrice de corrélation | jointure des 3 tables sur (city, jour) |
| Data | catalogue des tables (lignes, période, colonnes) et dernières lignes | toutes |

Sous chaque graphique, **SQL** montre la requête exécutée, le nombre de lignes et le temps de réponse.
Les nouveaux fichiers écrits par les collecteurs et les jobs Spark apparaissent à la requête suivante
(bouton « Refresh data » pour vider le cache du navigateur).

## Données

- Météo : `curated/noaa/weather_city_hourly` (Dev 1) s'il est présent dans le lac, sinon la copie locale Open-Meteo.
- Vélos : historique CityBikes (nov. 2024 →), historique Vélo'v de la Métropole de Lyon (janv.–oct. 2024,
  `pipelines/streaming_citybikes/velov_history.py`), collecteur temps réel, trajets V'Lille ilévia
  (avril 2025 →, `pipelines/streaming_citybikes/vlille_trips.py`). Paris et Lille n'ont pas d'historique public avant nov. 2024.
- Pollution : OpenAQ (Dev 3).
