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

Prérequis : Git, Docker Desktop (démarré), environ 10 Go libres pour les images.

1. Cloner le dépôt, puis se placer à sa racine.
2. Copier `.env.example` en `.env` : `copy .env.example .env` (Windows) ou `cp .env.example .env`.
3. Lancer l'infrastructure (10 à 20 min la première fois, le temps de télécharger et construire les images) :
   ```
   docker compose -f infra/docker-compose.yml --env-file .env up -d --build
   ```
4. Vérifier que tout fonctionne :
   ```
   docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 infra/check_infra.py
   ```
5. Arrêter : `docker compose -f infra/docker-compose.yml --env-file .env down`
   (les données restent ; ajouter `-v` efface aussi les données de MinIO et PostgreSQL).

| Service | Adresse | Identifiants (par défaut) |
|---|---|---|
| MinIO (Data Lake, console web) | http://localhost:9001 | `urbanhub` / `urbanhub123` |
| Spark master | http://localhost:8080 | |
| Spark worker | http://localhost:8081 | |
| Jupyter Lab (notebooks, jobs Spark) | http://localhost:8888 | |
| Superset (dashboard) | http://localhost:8088 | `admin` / `admin` |
| PostgreSQL | `localhost:5432`, base `urbanhub` | `urbanhub` / `urbanhub123` |

**Où lancer quoi ?**
- Les **jobs Spark et les notebooks** tournent dans le conteneur `jupyter`, où le projet est monté
  dans `/app`. Cela évite d'installer Java et Spark sur Windows. Dans ce conteneur, les adresses
  sont `http://minio:9000`, `postgres:5432` et `spark://spark-master:7077` (déjà configurées).
- Les **collecteurs** (simples appels d'API) peuvent tourner sur le PC avec le Python local ;
  ils joignent MinIO sur `http://localhost:9000`.

## Avancement

- **Développeur 2 (CityBikes)** : collecteur, clean, curated, notebook Partie 2 → [`docs/dev2_citybikes.md`](docs/dev2_citybikes.md)

## Règles Git

- On ne pousse jamais directement sur `main` : une branche par tâche
  (`dev1/...`, `dev2/...`, `dev3/...`, `common/...`) puis une pull request.
- Jamais de données, de clé API ni de fichier `.env` dans Git.
