# Dashboard Superset « UrbanHub »

http://localhost:8088/superset/dashboard/urbanhub/ (admin / admin)

5 onglets, 78 graphiques, une barre de filtres commune. Tout est décrit en code : on peut le
reconstruire à l'identique sur n'importe quel PC de l'équipe.

## Reconstruire ou installer le dashboard

Infra lancée (`docker compose -f infra/docker-compose.yml --env-file .env up -d`), tables publiées dans PostgreSQL :

```bash
# 1. profil des stations vélo (table station_profile, ~2 400 lignes) : une fois, puis le pipeline le recalcule chaque jour
docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m dashboard.superset.station_profile --force
# 2. datasets + graphiques + dashboard + filtres, puis vérification de chaque requête (1-2 min)
docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m dashboard.superset.build_dashboard
```

Le script peut être relancé sans risque : il remplace les graphiques du dashboard par ceux définis dans le code.
Une modification faite à la souris dans Superset est donc perdue au prochain lancement : la reporter dans
`build_dashboard.py`.

Autre possibilité, sans script : Superset → Dashboards → Import → zipper le dossier `export/` (export standard
Superset, YAML ; le mot de passe de la base est masqué, à ressaisir à l'import : `urbanhub123`).

## Fichiers

| Fichier | Rôle |
|---|---|
| `build_dashboard.py` | onglets, graphiques, filtres, couleurs (API REST de Superset) |
| `datasets.py` | vues SQL ajoutées aux tables du contrat : heure de Paris, jours, agrégats journaliers, seuils OMS, classes météo, corrélations |
| `station_profile.py` | résume `curated/citybikes/station_hourly` (29 M lignes, trop lourde pour PostgreSQL) en une ligne par station sur 12 mois |
| `export/` | export YAML du dashboard (versionné) |

## Filtres (barre de gauche, communs à tous les onglets)

| Filtre | Effet | Par défaut |
|---|---|---|
| Période | dates affichées | 12 derniers mois |
| Granularité | pas de temps des courbes : heure, jour, semaine, mois… | semaine |
| Ville | paris, lyon, lille (une ou plusieurs) | toutes |
| Type de jour | semaine / week-end | tous |
| Polluant | onglet Pollution seulement | PM2.5 |

Cliquer sur une série (ex. une ville) filtre aussi les autres graphiques (cross-filtering). Le dashboard se
rafraîchit tout seul toutes les heures. Quelques graphiques gardent volontairement leurs propres réglages
(indiqué dans leur titre ou leur en-tête) : les indicateurs « en direct » (7 derniers jours, heure par heure),
la visibilité (mesurée seulement jusqu'en août 2025), les graphiques « par mois ».

## Ce que montre chaque onglet (questions métier du guide)

| Onglet | Questions | Graphiques clés |
|---|---|---|
| Vue d'ensemble | état de la plateforme | dernière valeur + tendance 7 j, fraîcheur et couverture des données par source |
| Météo (Partie 1) | évolution saisonnière · périodes anormales · météo ↔ visibilité · jours extrêmes | température par ville, carte de chaleur mois × heure, écart à la normale du mois, jours anormaux, visibilité selon pluie / vent, top 10 jours chauds / froids / pluvieux / venteux |
| Mobilité (Partie 2) | pics journaliers · stations les plus utilisées · stations critiques · déséquilibres géographiques · zones sous-équipées | profil horaire semaine / week-end, carte de chaleur jour × heure, top stations, stations vides / pleines, carte de chaque ville (stations qui se vident / se remplissent en journée), zones de ~1 km à forte rotation |
| Pollution (Partie 3) | zones les plus polluées · variation journalière · polluants dominants · épisodes anormaux | concentration par ville, profils horaires, indice = concentration / valeur guide OMS par polluant, jours de dépassement, épisodes les plus forts |
| Analyses croisées (Partie 4) | météo → pollution · météo → vélos · conditions favorables · forte interaction | usage selon température / pluie / vent, usage et température, PM2.5 / NO2 selon le vent, O3 selon la chaleur, corrélations, jours favorables, jours de forte exposition des cyclistes |

## Choix et limites (à dire pendant la démo)

- **Heure de Paris** pour les profils horaires (les tables du contrat sont en UTC).
- **Normale** de température = moyenne 2024-2026 du même mois et de la même ville (3 ans seulement) ;
  jour « anormal » = écart d'au moins 1,5 écart-type.
- **Seuils OMS 2021** (moyenne 24 h) : PM2.5 15, PM10 45, NO2 25, CO 4 000 µg/m³ ; O3 : repère 100 µg/m³ (valeur 8 h).
- **Vélos** : données depuis nov. 2024, mois manquants à la source (ex. Paris avr.-juil. 2026, octobre 2026
  partout) ; les comparaisons entre villes se font en mouvements **par station**.
- **Pas de fond de carte** : la carte de Superset exige une clé Mapbox ; les stations sont placées par
  latitude / longitude, ce qui dessine bien la forme de chaque ville.
- **Jour favorable au vélo** = 12-25 °C, moins de 1 mm de pluie, vent < 6 m/s, PM2.5 ≤ 15 µg/m³.
