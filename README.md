# UrbanHub

Jumeau numérique urbain : plateforme Big Data qui ingère, stocke et analyse des données
de Smart City pour les grandes villes françaises.

| Flux | Source | Responsable |
|---|---|---|
| Batch (météo) | NOAA Global Hourly | Développeur 1 |
| Streaming (vélos) | CityBikes API v2 | Développeur 2 |
| IoT (pollution) + analyses croisées | OpenAQ API v3 | Développeur 3 |

Architecture : collecteurs Python → Data Lake MinIO (`raw` → `clean` → `curated`)
→ jobs Spark → PostgreSQL → dashboard Superset + notebooks Jupyter.

Le guide complet (rôles, contrat de données, étapes) est dans
[`docs/UrbanHub_Guide_Coordination.pdf`](docs/UrbanHub_Guide_Coordination.pdf).
Les décisions d'équipe sont dans [`docs/decisions.md`](docs/decisions.md).

## Démarrage rapide

> En construction : les commandes de lancement seront ajoutées avec `infra/docker-compose.yml`.

1. Cloner le dépôt.
2. Copier `.env.example` en `.env`.
3. *(à venir)* `docker compose -f infra/docker-compose.yml up -d`

## Avancement

- **Développeur 2 (CityBikes)** : collecteur, clean, curated, notebook Partie 2 → [`docs/dev2_citybikes.md`](docs/dev2_citybikes.md)

## Règles Git

- On ne pousse jamais directement sur `main` : une branche par tâche
  (`dev1/...`, `dev2/...`, `dev3/...`, `common/...`) puis une pull request.
- Jamais de données, de clé API ni de fichier `.env` dans Git.
