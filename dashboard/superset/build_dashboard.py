"""Construit (ou reconstruit) le dashboard Superset « UrbanHub » via l'API REST de Superset.

    python3 -m dashboard.superset.build_dashboard          # dans le conteneur jupyter (réseau Docker)

Ce que fait le script, à chaque lancement (il peut être relancé sans risque) :
  1. connexion à la base PostgreSQL « UrbanHub » (créée si absente) ;
  2. datasets : tables publiées par les pipelines + vues SQL de dashboard/superset/datasets.py ;
  3. supprime les anciens graphiques du dashboard, puis recrée tous ceux définis plus bas ;
  4. dashboard à 5 onglets (Vue d'ensemble, Météo, Mobilité, Pollution, Croisé) + barre de filtres
     (Période, Granularité, Ville, Type de jour, Polluant) ;
  5. vérifie que la requête de chaque graphique s'exécute sans erreur.

Variables : SUPERSET_URL (défaut http://superset:8088), SUPERSET_ADMIN_USER / SUPERSET_ADMIN_PASSWORD (admin / admin).
"""
import json
import os
import sys
import uuid

import requests

from dashboard.superset.datasets import DATASETS, VERBOSE

URL = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
PG = (f"postgresql://{os.environ.get('POSTGRES_USER', 'urbanhub')}:{os.environ.get('POSTGRES_PASSWORD', 'urbanhub123')}"
      f"@{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/{os.environ.get('POSTGRES_DB', 'urbanhub')}")
DB_NAME = "UrbanHub"
TITLE, SLUG = "UrbanHub", "urbanhub"

PHYSICAL = ["weather_city_hourly", "pollution_city_hourly", "mobility_city_hourly", "urban_city_hourly", "station_profile"]
TIME_COLUMN = {"station_profile": None, "v_station_zones": None, "v_station_map": None, "v_correlations": None}

# Couleurs fixes, identiques dans tous les onglets (guide : « mêmes couleurs par thème »)
LABEL_COLORS = {
    "paris": "#1F6FD1", "lyon": "#E4572E", "lille": "#2A9D8F",
    "Semaine": "#4C5B70", "Week-end": "#F2A541",
    "PM2.5": "#7B2CBF", "PM10": "#C77DFF", "NO2": "#E07A1F", "O3": "#3A86FF", "CO": "#8D99AE",
    "Météo": "#1F6FD1", "Vélos": "#2A9D8F", "Pollution": "#7B2CBF", "Croisé": "#E4572E",
    "Favorable": "#2A9D8F", "Défavorable": "#ADB5BD",
    "Anormalement chaud": "#D62828", "Anormalement froid": "#3A86FF", "Normal": "#ADB5BD",
    "Se vide en journée": "#D62828", "Se remplit en journée": "#1F6FD1", "Équilibrée": "#ADB5BD",
    "Vélos": "#2A9D8F",
}


# Palette « supersetColors » de Superset 5.0. Sans elle dans color_scheme_domain, Superset considère que la
# palette a changé et efface label_colors (nos couleurs fixes) au premier affichage.
SCHEME_COLORS = ["#1FA8C9", "#454E7C", "#5AC189", "#FF7F44", "#666666", "#E04355", "#FCC700", "#A868B7", "#3CCCCB",
                 "#A38F79", "#8FD3E4", "#A1A6BD", "#ACE1C4", "#FEC0A1", "#B2B2B2", "#EFA1AA", "#FDE380", "#D3B3DA",
                 "#9EE5E5", "#D1C6BC"]


# ============================================================================ API
class Superset:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(f"{URL}/api/v1/security/login",
                        json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True})
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{URL}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": URL})

    def call(self, method: str, path: str, **kw):
        r = self.s.request(method, f"{URL}/api/v1/{path}", **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code} {r.text[:800]}")
        return r.json() if r.content else {}

    def find(self, resource: str, col: str, value: str):
        q = f"(filters:!((col:{col},opr:eq,value:'{value}')),page_size:100)"
        return self.call("GET", f"{resource}/?q={q}")["result"]


# ============================================================================ datasets
def ensure_database(api: Superset) -> int:
    found = api.find("database", "database_name", DB_NAME)
    if found:
        return found[0]["id"]
    return api.call("POST", "database/", json={"database_name": DB_NAME, "sqlalchemy_uri": PG,
                                                "expose_in_sqllab": True})["id"]


def ensure_dataset(api: Superset, db_id: int, name: str, sql: str | None = None) -> int:
    found = [d for d in api.find("dataset", "table_name", name) if d["database"]["id"] == db_id]
    if found:
        ds_id = found[0]["id"]
        if sql is not None:
            api.call("PUT", f"dataset/{ds_id}?override_columns=true", json={"sql": sql})
    else:
        body = {"database": db_id, "schema": "public", "table_name": name}
        if sql is not None:
            body["sql"] = sql
        ds_id = api.call("POST", "dataset/", json=body)["id"]
    api.call("PUT", f"dataset/{ds_id}/refresh")
    # colonne de temps principale, noms affichés
    ds = api.call("GET", f"dataset/{ds_id}")["result"]
    time_col = TIME_COLUMN.get(name, "timestamp")
    columns = []
    for c in ds["columns"]:
        col = {k: c.get(k) for k in ("id", "column_name", "type", "expression", "description", "groupby",
                                     "filterable", "python_date_format")}
        col["is_dttm"] = c["column_name"] == time_col or bool(c.get("is_dttm"))
        col["verbose_name"] = VERBOSE.get(c["column_name"])
        columns.append({k: v for k, v in col.items() if v is not None})
    api.call("PUT", f"dataset/{ds_id}", json={"main_dttm_col": time_col, "columns": columns,
                                              "cache_timeout": 600})
    return ds_id


# ============================================================================ briques de graphiques
def M(sql: str, label: str) -> dict:
    """Métrique SQL libre."""
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "optionName": f"metric_{uuid.uuid4().hex[:8]}"}


def time_filter(col: str = "timestamp") -> list:
    return [{"clause": "WHERE", "subject": col, "operator": "TEMPORAL_RANGE", "comparator": "No filter",
             "expressionType": "SIMPLE"}]


def where(sql: str) -> dict:
    return {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": sql}


BASE = {"color_scheme": "supersetColors", "row_limit": 50000}
AXES = {"show_legend": True, "legendType": "scroll", "legendOrientation": "top", "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date", "x_axis_time_format": "smart_date", "truncateYAxis": False,
        "y_axis_title_margin": 40, "y_axis_title_position": "Left", "zoomable": False, "markerEnabled": False,
        "sort_series_type": "name", "sort_series_ascending": True}


def series(ds, metrics, groupby=("city",), x="timestamp", grain="P1W", kind="line", y_fmt="SMART_NUMBER",
           y_title="", filters=(), stack=False, horizontal=False, extra=None):
    """Courbe ou barres : axe X temporel (granularité modifiable par le filtre) ou catégoriel."""
    p = {**BASE, **AXES, "viz_type": "echarts_timeseries_bar" if kind == "bar" else "echarts_timeseries_line",
         "datasource": ds, "x_axis": x, "metrics": metrics, "groupby": list(groupby),
         "adhoc_filters": time_filter() + list(filters), "y_axis_format": y_fmt, "y_axis_title": y_title,
         "order_desc": True}
    if x == "timestamp":
        p["time_grain_sqla"] = grain
    elif horizontal:
        p.update({"x_axis_sort_asc": True})
    else:   # axe catégoriel (classes, heures) : toutes les étiquettes, inclinées
        p.update({"x_axis_sort_asc": True, "xAxisLabelRotation": 30, "xAxisLabelInterval": 0})
    if stack:
        p["stack"] = "Stack"
    if horizontal:
        p["orientation"] = "horizontal"
    p.update(extra or {})
    return p


def kpi(ds, metric, fmt=".1f", grain="PT1H", filters=(), lag=24, suffix="vs il y a 24 h",
        window='DATEADD(DATETIME("now"), -7, day) : now'):
    """Dernière valeur horaire + tendance sur 7 jours (indépendant des filtres Période et Granularité)."""
    return {**BASE, "viz_type": "big_number", "datasource": ds, "x_axis": "timestamp", "time_grain_sqla": grain,
            "metric": metric, "adhoc_filters": [{**time_filter()[0], "comparator": window}] + list(filters),
            "_no_time_filter": True, "_no_grain_filter": True, "show_trend_line": True,
            "start_y_axis_at_zero": False, "y_axis_format": fmt, "compare_lag": lag, "compare_suffix": suffix,
            "show_timestamp": True, "time_format": "smart_date", "header_font_size": 0.35,
            "subheader_font_size": 0.13, "rolling_type": "None"}


def total(ds, metric, subheader, fmt=".1f", filters=(), time_col="timestamp"):
    """Grand nombre sur toute la période filtrée."""
    return {**BASE, "viz_type": "big_number_total", "datasource": ds, "metric": metric, "subheader": subheader,
            "adhoc_filters": (time_filter(time_col) if time_col else []) + list(filters), "y_axis_format": fmt,
            "header_font_size": 0.35, "subheader_font_size": 0.13}


def heatmap(ds, x, y, metric, fmt="SMART_NUMBER", scheme="oranges", filters=(), time=True):
    return {**BASE, "viz_type": "heatmap_v2", "datasource": ds, "x_axis": x, "groupby": y, "metric": metric,
            "adhoc_filters": (time_filter() if time else []) + list(filters), "linear_color_scheme": scheme,
            "normalize_across": "heatmap", "legend_type": "continuous", "show_legend": True, "show_values": False,
            "show_percentage": False, "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
            "y_axis_format": fmt, "xscale_interval": 3, "yscale_interval": 1, "left_margin": "auto",
            "bottom_margin": "auto", "value_bounds": [None, None]}


def table(ds, columns, order_by, desc=True, limit=10, filters=(), time=True, formats=None, bars=False,
          ts_format="%d/%m/%Y"):
    """Tableau de lignes brutes, trié (les filtres du dashboard s'appliquent)."""
    return {**BASE, "viz_type": "table", "datasource": ds, "query_mode": "raw", "all_columns": columns,
            "order_by_cols": [json.dumps([order_by, not desc])], "row_limit": limit, "server_page_length": 10,
            "adhoc_filters": (time_filter() if time else []) + list(filters) + [where(f"{order_by} IS NOT NULL")],
            "table_timestamp_format": ts_format,
            "include_search": False, "show_cell_bars": bars, "page_length": 0, "align_pn": False,
            "color_pn": False, "allow_rearrange_columns": True,
            "column_config": {c: {"d3NumberFormat": f} for c, f in (formats or {}).items()}}


def agg_table(ds, groupby, metrics, sort_metric, desc=True, limit=20, filters=(), time=True, formats=None):
    """Tableau agrégé (une ligne par groupe), trié sur une métrique."""
    return {**BASE, "viz_type": "table", "datasource": ds, "query_mode": "aggregate", "groupby": groupby,
            "metrics": metrics, "timeseries_limit_metric": sort_metric, "order_desc": desc, "row_limit": limit,
            "adhoc_filters": (time_filter() if time else []) + list(filters), "include_search": False,
            "show_cell_bars": True, "page_length": 0, "table_timestamp_format": "%d/%m/%Y",
            "column_config": {c: {"d3NumberFormat": f} for c, f in (formats or {}).items()}}


def pie(ds, groupby, metric, filters=(), time=True):
    return {**BASE, "viz_type": "pie", "datasource": ds, "groupby": groupby, "metric": metric,
            "adhoc_filters": (time_filter() if time else []) + list(filters), "show_legend": True,
            "legendOrientation": "right", "show_labels": True, "label_type": "key_percent", "number_format": "SMART_NUMBER",
            "donut": True, "innerRadius": 40, "outerRadius": 75, "sort_by_metric": True, "labels_outside": True}


def bubble(ds, entity, x, y, size, series_col="city", filters=(), time=False, x_label="", y_label="",
           x_fmt="SMART_NUMBER", y_fmt="SMART_NUMBER", max_size="25", limit=5000):
    return {**BASE, "viz_type": "bubble_v2", "datasource": ds, "entity": entity, "x": x, "y": y, "size": size,
            "series": series_col, "adhoc_filters": (time_filter() if time else []) + list(filters),
            "max_bubble_size": max_size, "row_limit": limit, "show_legend": True, "legendOrientation": "top",
            "x_axis_label": x_label, "y_axis_label": y_label, "xAxisFormat": x_fmt, "y_axis_format": y_fmt,
            "tooltipSizeFormat": "SMART_NUMBER", "opacity": 0.6, "truncateXAxis": True, "truncateYAxis": True,
            "xAxisBounds": [None, None], "yAxisBounds": [None, None], "xAxisLabelRotation": 0,
            "x_axis_title_margin": 30, "y_axis_title_margin": 50, "logXAxis": False, "logYAxis": False}


def own(params: dict, period: bool = False, grain: bool = False) -> dict:
    """Le graphique garde sa propre période (period) et/ou son propre pas de temps (grain)."""
    if period:
        params["_no_time_filter"] = True
    if grain:
        params["_no_grain_filter"] = True
    return params


def mixed(ds, metric_a, metric_b, kind_a="bar", kind_b="line", grain="P1W", fmt_a="SMART_NUMBER", fmt_b=".1f"):
    return {**BASE, "viz_type": "mixed_timeseries", "datasource": ds, "x_axis": "timestamp", "time_grain_sqla": grain,
            "metrics": [metric_a], "groupby": [], "adhoc_filters": time_filter(),
            "metrics_b": [metric_b], "groupby_b": [], "adhoc_filters_b": time_filter(),
            "seriesType": kind_a, "seriesTypeB": kind_b, "yAxisIndex": 0, "yAxisIndexB": 1,
            "show_legend": True, "legendOrientation": "top", "rich_tooltip": True, "zoomable": False,
            "y_axis_format": fmt_a, "y_axis_format_secondary": fmt_b, "row_limit": 50000, "row_limit_b": 50000,
            "order_desc": True, "order_desc_b": True, "x_axis_time_format": "smart_date",
            "tooltipTimeFormat": "smart_date", "markerEnabled": False, "markerEnabledB": False}


# ============================================================================ le dashboard
def layout(d: dict) -> list:
    """Onglets -> lignes -> éléments. Un élément = ("md", texte, largeur, hauteur) ou ("chart", nom, params, largeur, hauteur)."""
    W, P, B, X, O = d["v_weather_daily"], d["v_pollution_long"], d["v_mobility_hourly"], d["v_urban_daily"], d["v_data_freshness"]
    WH, PD, UH, SP, SZ = d["v_weather_hourly"], d["v_pollution_daily"], d["v_urban_hourly"], d["station_profile"], d["v_station_zones"]
    SM = d["v_station_map"]
    WR, MR, PR, COV, COR = d["weather_city_hourly"], d["mobility_city_hourly"], d["pollution_city_hourly"], d["v_data_coverage"], d["v_correlations"]
    favorable = where("conditions_velo = 'Favorable'")
    bikes = where("usage_per_hour IS NOT NULL")

    return [
        # ------------------------------------------------------------------ VUE D'ENSEMBLE
        ("Vue d'ensemble", [
            [("md", """## UrbanHub · Paris, Lyon, Lille
Plateforme Big Data Smart City : **météo** (NOAA puis Open-Meteo, batch), **vélos en libre-service** (CityBikes, streaming toutes les 2 min),
**qualité de l'air** (OpenAQ, IoT toutes les 15 min), croisés heure par heure. Données mises à jour automatiquement toutes les heures.

**Mode d'emploi :** la barre de **filtres à gauche** s'applique à tous les onglets : *Période*, *Granularité* (heure, jour, semaine, mois),
*Ville*, *Type de jour* (semaine / week-end) et, dans l'onglet Pollution, *Polluant*. Cliquer sur une ville dans un graphique filtre les
autres graphiques. Survoler une courbe affiche les valeurs.""", 12, 22)],
            [("md", "#### En direct : dernière heure disponible et tendance des 7 derniers jours (filtre Ville actif)", 12, 6)],
            [("chart", "Température (°C)", kpi(WR, M("AVG(temperature)", "Température (°C)")), 3, 45),
             ("chart", "Vélos : mouvements par heure", kpi(MR, M("SUM(usage_events)", "Mouvements / heure"), fmt=",.0f"), 3, 45),
             ("chart", "PM2.5 (µg/m³) · seuil OMS 15", kpi(PR, M("AVG(pm25)", "PM2.5 (µg/m³)")), 3, 45),
             ("chart", "NO2 (µg/m³) · seuil OMS 25", kpi(PR, M("AVG(no2)", "NO2 (µg/m³)")), 3, 45)],
            [("chart", "Fraîcheur des données : dernière heure reçue par source et par ville (UTC)",
              own(table(O, ["source", "city", "derniere_heure", "premiere_heure", "nb_lignes", "table_name"], "source",
                        desc=False, limit=20, time=False, ts_format="%d/%m/%Y %H:%M"), period=True), 6, 62),
             ("chart", "Couverture des données : heures disponibles par source (3 villes)",
              series(COV, [M("COUNT(*)", "Heures (3 villes)")], groupby=["source"], kind="bar", grain="P1M",
                     y_title="heures × villes"), 6, 62)],
        ]),
        # ------------------------------------------------------------------ MÉTÉO
        ("Météo", [
            [("md", """### Partie 1 · Météo
**Questions :** évolution saisonnière de la température · périodes anormales · lien météo ↔ visibilité · jours extrêmes.
Source : NOAA Global Hourly (stations à moins de 30 km) jusqu'au 24/08/2025, puis Open-Meteo (modèle ~10 km).""", 12, 14)],
            [("chart", "Moyenne", total(W, M("AVG(tmean)", "°C"), "°C sur la période"), 2, 30),
             ("chart", "Maximum", total(W, M("MAX(tmax)", "°C"), "°C (relevé horaire)"), 2, 30),
             ("chart", "Minimum", total(W, M("MIN(tmin)", "°C"), "°C (relevé horaire)"), 2, 30),
             ("chart", "Pluie", total(W, M("AVG(precip_mm)", "mm"), "mm par jour en moyenne"), 2, 30),
             ("chart", "Jours trop chauds", total(W, M("COUNT(*)", "jours"), "jours × villes (écart ≥ 1,5 σ)", fmt=",d",
                                                  filters=[where("anomalie = 'Anormalement chaud'")]), 2, 30),
             ("chart", "Jours trop froids", total(W, M("COUNT(*)", "jours"), "jours × villes (écart ≤ -1,5 σ)", fmt=",d",
                                                  filters=[where("anomalie = 'Anormalement froid'")]), 2, 30)],
            [("md", "#### Évolution saisonnière", 12, 6)],
            [("chart", "Température moyenne par ville (°C)",
              series(WR, [M("AVG(temperature)", "Température (°C)")], y_fmt=".1f", y_title="°C"), 8, 55),
             ("chart", "Température par mois et par heure de la journée (°C)",
              heatmap(WH, "heure", "mois", M("AVG(temperature)", "°C"), fmt=".1f", scheme="oranges"), 4, 55)],
            [("chart", "Températures minimales et maximales journalières (°C)",
              series(W, [M("AVG(tmax)", "Max (°C)"), M("AVG(tmin)", "Min (°C)")], groupby=[], y_fmt=".1f", y_title="°C"), 6, 50),
             ("chart", "Température moyenne par saison et par ville (°C)",
              series(WH, [M("AVG(temperature)", "°C")], x="saison", kind="bar", y_fmt=".1f", y_title="°C"), 6, 50)],
            [("md", "#### Périodes anormales : écart à la normale du mois (moyenne 2024-2026 de la ville)", 12, 6)],
            [("chart", "Écart de température à la normale (°C) : au-dessus de 0 = plus chaud que d'habitude",
              series(W, [M("AVG(t_anomalie)", "Écart (°C)")], kind="bar", grain="P1D", y_fmt="+.1f", y_title="°C"), 8, 50),
             ("chart", "Jours anormaux par mois",
              own(series(W, [M("COUNT(*)", "jours")], groupby=["anomalie"], kind="bar", grain="P1M", stack=True,
                         filters=[where("anomalie <> 'Normal'")], y_fmt=",d"), grain=True), 4, 50)],
            [("chart", "Liste des jours anormaux (les plus forts écarts d'abord)",
              table(W, ["timestamp", "city", "anomalie", "tmean", "t_normale", "t_anomalie", "z_score"], "z_score",
                    limit=50, filters=[where("anomalie <> 'Normal'")],
                    formats={"tmean": ".1f", "t_normale": ".1f", "t_anomalie": "+.1f", "z_score": "+.1f"}), 12, 45)],
            [("md", "#### Météo ↔ visibilité · mesurée par les stations NOAA de 2024 à août 2025 : ces 3 graphiques ignorent le filtre Période", 12, 6)],
            [("chart", "Visibilité moyenne selon l'intensité de la pluie (m)",
              own(series(WH, [M("AVG(visibility)", "Visibilité (m)")], x="pluie", kind="bar", y_fmt=",.0f", y_title="m",
                         filters=[where("visibility IS NOT NULL")]), period=True), 4, 50),
             ("chart", "Visibilité moyenne selon le vent (m)",
              own(series(WH, [M("AVG(visibility)", "Visibilité (m)")], x="vent", kind="bar", y_fmt=",.0f", y_title="m",
                         filters=[where("visibility IS NOT NULL")]), period=True), 4, 50),
             ("chart", "Visibilité par mois et par heure (m) : les brouillards du matin en hiver",
              own(heatmap(WH, "heure", "mois", M("AVG(visibility)", "m"), fmt=",.0f", scheme="blue_white_yellow",
                          filters=[where("visibility IS NOT NULL")]), period=True), 4, 50)],
            [("md", "#### Pluie et vent", 12, 6)],
            [("chart", "Pluie cumulée par ville (mm)",
              series(WR, [M("SUM(precipitation)", "Pluie (mm)")], kind="bar", grain="P1M", y_fmt=",.0f", y_title="mm"), 6, 50),
             ("chart", "Vent moyen par ville (m/s)",
              series(WR, [M("AVG(wind_speed)", "Vent (m/s)")], y_fmt=".1f", y_title="m/s"), 6, 50)],
            [("md", "#### Jours extrêmes (les filtres Période et Ville s'appliquent)", 12, 6)],
            [("chart", "Les 10 jours les plus chauds",
              table(W, ["timestamp", "city", "tmax", "tmean"], "tmax", formats={"tmax": ".1f", "tmean": ".1f"}), 3, 50),
             ("chart", "Les 10 jours les plus froids",
              table(W, ["timestamp", "city", "tmin", "tmean"], "tmin", desc=False, formats={"tmin": ".1f", "tmean": ".1f"}), 3, 50),
             ("chart", "Les 10 jours les plus pluvieux",
              table(W, ["timestamp", "city", "precip_mm"], "precip_mm", formats={"precip_mm": ".1f"}), 3, 50),
             ("chart", "Les 10 jours les plus venteux",
              table(W, ["timestamp", "city", "wind_max", "wind_mean"], "wind_max", formats={"wind_max": ".1f", "wind_mean": ".1f"}), 3, 50)],
        ]),
        # ------------------------------------------------------------------ MOBILITÉ
        ("Mobilité", [
            [("md", """### Partie 2 · Vélos en libre-service (Vélib', Vélo'v, V'Lille)
**Questions :** pics journaliers · stations les plus utilisées · stations critiques (vides ou pleines) · déséquilibres géographiques · zones sous-équipées.
*Mouvements* = vélos pris + vélos rendus (variation du nombre de vélos entre deux relevés). Données depuis nov. 2024 (Lyon : historique 2024 partiel) ;
les mois sans données à la source apparaissent vides.""", 12, 16)],
            [("chart", "Mouvements par heure (moyenne)", total(B, M("AVG(usage_events)", "mouvements / h"), "mouvements / heure", fmt=",.0f"), 3, 30),
             ("chart", "Taux de remplissage moyen", total(B, M("AVG(occupancy_rate)", "remplissage"), "des places occupées par un vélo", fmt=".0%"), 3, 30),
             ("chart", "Stations presque vides", total(B, M("AVG(pct_empty)", "vides"), "des stations en moyenne (< 10 %)", fmt=".1%"), 3, 30),
             ("chart", "Stations presque pleines", total(B, M("AVG(pct_full)", "pleines"), "des stations en moyenne (> 90 %)", fmt=".1%"), 3, 30)],
            [("md", "#### Pics journaliers", 12, 6)],
            [("chart", "Mouvements par heure de la journée : semaine vs week-end (heure de Paris)",
              series(B, [M("AVG(usage_events)", "Mouvements / h")], x="heure", groupby=["type_jour"], y_fmt=",.0f",
                     y_title="mouvements / h"), 6, 50),
             ("chart", "Mouvements par jour de la semaine et par heure",
              heatmap(B, "heure", "jour", M("AVG(usage_events)", "mouvements / h"), fmt=",.0f", scheme="oranges"), 6, 50)],
            [("chart", "Mouvements de vélos par ville (moyenne horaire)",
              series(B, [M("AVG(usage_events)", "Mouvements / h")], y_fmt=",.0f", y_title="mouvements / h"), 8, 50),
             ("chart", "Mouvements par station et par heure, selon la ville",
              series(B, [M("AVG(usage_per_station)", "Mouvements / station / h")], x="heure", y_fmt=".2f"), 4, 50)],
            [("md", "#### Disponibilité : stations vides et pleines", 12, 6)],
            [("chart", "Part des stations presque vides (< 10 %)",
              series(B, [M("AVG(pct_empty)", "Stations vides")], y_fmt=".0%", y_title="% des stations"), 6, 45),
             ("chart", "Part des stations presque pleines (> 90 %)",
              series(B, [M("AVG(pct_full)", "Stations pleines")], y_fmt=".0%", y_title="% des stations"), 6, 45)],
            [("md", "#### Stations (profil calculé sur les 12 derniers mois de données · filtre Ville actif)", 12, 6)],
            [("chart", "Top 15 des stations les plus utilisées",
              table(SP, ["station_name", "city", "usage_per_day", "capacity", "occupancy_rate"], "usage_per_day", limit=15,
                    time=False, bars=True, filters=[where("nb_hours >= 720 AND capacity > 0")],
                    formats={"usage_per_day": ",.0f", "capacity": ",.0f", "occupancy_rate": ".0%"}), 6, 55),
             ("chart", "Stations critiques à rééquilibrer (souvent vides ou pleines)",
              agg_table(SP, ["station_name", "city"],
                        [M("AVG(pct_empty) + AVG(pct_full)", "Vide ou pleine"), M("AVG(pct_empty)", "Vide"),
                         M("AVG(pct_full)", "Pleine"), M("AVG(usage_per_day)", "Mouvements / jour")],
                        M("AVG(pct_empty) + AVG(pct_full)", "Vide ou pleine"), limit=15, time=False,
                        filters=[where("nb_hours >= 720 AND capacity > 0 AND usage_per_day >= 5 AND pct_empty IS NOT NULL AND pct_full IS NOT NULL")],
                        formats={"Vide ou pleine": ".0%", "Vide": ".0%", "Pleine": ".0%", "Mouvements / jour": ",.0f"}), 6, 55)],
            [("md", """#### Déséquilibres géographiques : cartes des stations
Chaque point est une station (taille = mouvements par jour). **Rouge** : la station se vide en journée (quartier résidentiel, départs le matin) ·
**bleu** : elle se remplit (bureaux, gares, universités) · gris : équilibrée. Les camions de rééquilibrage vont des zones bleues vers les rouges.""", 12, 12)],
            [("chart", f"Paris : profil des stations", bubble(SM, "station_name", M("AVG(longitude)", "Longitude"),
                     M("AVG(latitude)", "Latitude"), M("SUM(usage_per_day)", "Mouvements / jour"), series_col="profil",
                     filters=[where("city = 'paris'")], x_fmt=".2f", y_fmt=".2f", max_size="14"), 4, 70),
             ("chart", f"Lyon : profil des stations", bubble(SM, "station_name", M("AVG(longitude)", "Longitude"),
                     M("AVG(latitude)", "Latitude"), M("SUM(usage_per_day)", "Mouvements / jour"), series_col="profil",
                     filters=[where("city = 'lyon'")], x_fmt=".2f", y_fmt=".2f", max_size="14"), 4, 70),
             ("chart", f"Lille : profil des stations", bubble(SM, "station_name", M("AVG(longitude)", "Longitude"),
                     M("AVG(latitude)", "Latitude"), M("SUM(usage_per_day)", "Mouvements / jour"), series_col="profil",
                     filters=[where("city = 'lille'")], x_fmt=".2f", y_fmt=".2f", max_size="14"), 4, 70)],
            [("chart", "Stations qui se vident en journée (départs le matin)",
              table(SM, ["station_name", "city", "occ_night", "occ_day"], "imbalance", desc=False, limit=15,
                    time=False, formats={"occ_night": ".0%", "occ_day": ".0%", "imbalance": "+.0%"}), 4, 55),
             ("chart", "Stations qui se remplissent en journée (arrivées le matin)",
              table(SM, ["station_name", "city", "occ_night", "occ_day"], "imbalance", limit=15,
                    time=False, formats={"occ_night": ".0%", "occ_day": ".0%", "imbalance": "+.0%"}), 4, 55),
             ("chart", "Profil des stations par ville",
              series(SM, [M("COUNT(*)", "Stations")], x="city", groupby=["profil"], kind="bar", stack=True, y_fmt=".0%",
                     extra={"adhoc_filters": [], "contributionMode": "row"}), 4, 55)],
            [("md", "#### Zones sous-équipées : carrés de ~1 km où la demande est forte pour peu de places (rotation élevée, stations souvent vides)", 12, 6)],
            [("chart", "Zones les plus sous-équipées",
              table(SZ, ["zone", "nb_stations", "capacity", "usage_per_day", "rotation", "pct_empty"], "rotation", limit=20,
                    time=False, bars=True, filters=[where("nb_stations >= 2")],
                    formats={"capacity": ",.0f", "usage_per_day": ",.0f", "rotation": ".1f", "pct_empty": ".0%"}), 7, 60),
             ("chart", "Rotation moyenne par ville (mouvements par place et par jour)",
              series(SZ, [M("SUM(usage_per_day) / NULLIF(SUM(capacity), 0)", "Rotation")], x="city", groupby=[],
                     kind="bar", y_fmt=".1f", extra={"adhoc_filters": []}), 5, 60)],
        ]),
        # ------------------------------------------------------------------ POLLUTION
        ("Pollution", [
            [("md", """### Partie 3 · Qualité de l'air (stations officielles OpenAQ)
**Questions :** zones les plus polluées · variation journalière · polluants dominants · épisodes anormaux.
Choisir le **polluant** dans la barre de filtres (PM2.5 par défaut). *Indice* = concentration / valeur guide OMS 2021 (moyenne 24 h :
PM2.5 15, PM10 45, NO2 25, CO 4 000 µg/m³ ; O3 : repère 100 µg/m³ sur 8 h). Un indice > 1 = au-dessus de la recommandation OMS.""", 12, 16)],
            [("chart", "Concentration moyenne", total(P, M("AVG(value)", "µg/m³"), "µg/m³ (polluant choisi)"), 3, 30),
             ("chart", "Indice OMS moyen", total(P, M("AVG(indice)", "indice"), "× la valeur guide OMS", fmt=".2f"), 3, 30),
             ("chart", "Jours au-dessus du seuil OMS", total(PD, M("SUM(depassement)", "jours"), "jours × villes", fmt=",d"), 3, 30),
             ("chart", "Pic horaire", total(P, M("MAX(value)", "µg/m³"), "µg/m³ (maximum horaire)", fmt=",.0f"), 3, 30)],
            [("md", "#### Évolution et zones les plus polluées", 12, 6)],
            [("chart", "Concentration par ville (µg/m³)",
              series(P, [M("AVG(value)", "µg/m³")], y_fmt=".1f", y_title="µg/m³"), 8, 50),
             ("chart", "Concentration moyenne par ville (µg/m³)",
              series(P, [M("AVG(value)", "µg/m³")], x="city", groupby=[], kind="bar", y_fmt=".1f"), 4, 50)],
            [("md", "#### Variation journalière", 12, 6)],
            [("chart", "Profil horaire : semaine vs week-end (heure de Paris)",
              series(P, [M("AVG(value)", "µg/m³")], x="heure", groupby=["type_jour"], y_fmt=".1f", y_title="µg/m³"), 6, 50),
             ("chart", "Concentration par jour de la semaine et par heure",
              heatmap(P, "heure", "jour", M("AVG(value)", "µg/m³"), fmt=".1f", scheme="purples"), 6, 50)],
            [("chart", "Saisonnalité : concentration par mois et par heure",
              heatmap(P, "heure", "mois", M("AVG(value)", "µg/m³"), fmt=".1f", scheme="purples"), 6, 50),
             ("chart", "Profil horaire par ville",
              series(P, [M("AVG(value)", "µg/m³")], x="heure", y_fmt=".1f", y_title="µg/m³"), 6, 50)],
            [("md", "#### Polluants dominants (tous polluants, indépendamment du filtre Polluant)", 12, 6)],
            [("chart", "Indice OMS moyen par polluant et par ville (> 1 = au-dessus de la recommandation)",
              series(P, [M("AVG(indice)", "Indice OMS")], x="pollutant", kind="bar", y_fmt=".2f", y_title="× seuil OMS"), 6, 50),
             ("chart", "Jours au-dessus du seuil OMS par polluant",
              series(PD, [M("SUM(depassement)", "jours")], x="pollutant", kind="bar", y_fmt=",d", y_title="jours"), 6, 50)],
            [("md", "#### Épisodes anormaux", 12, 6)],
            [("chart", "Jours au-dessus du seuil OMS, par mois (polluant choisi)",
              own(series(PD, [M("SUM(depassement)", "jours")], kind="bar", grain="P1M", stack=True, y_fmt=",d",
                         y_title="jours"), grain=True), 6, 50),
             ("chart", "Épisodes les plus forts (moyenne du jour / seuil OMS)",
              table(PD, ["timestamp", "city", "pollutant", "moyenne", "pic", "seuil", "indice"], "indice", limit=30,
                    formats={"moyenne": ".1f", "pic": ".0f", "seuil": ".0f", "indice": ".2f"}), 6, 50)],
        ]),
        # ------------------------------------------------------------------ CROISÉ
        ("Analyses croisées", [
            [("md", """### Partie 4 · Météo × vélos × pollution
**Questions :** effet de la météo sur la pollution · effet de la météo sur l'usage des vélos · conditions favorables à la mobilité douce ·
périodes de forte interaction. Table `urban_city_hourly` (jointure heure par heure des trois sources) agrégée par jour.
*Jour favorable* = 12-25 °C, moins de 1 mm de pluie, vent < 6 m/s et PM2.5 ≤ 15 µg/m³.""", 12, 16)],
            [("chart", "Jours favorables au vélo", total(X, M("AVG(CASE WHEN conditions_velo = 'Favorable' THEN 1.0 ELSE 0 END)", "part"),
                                                        "des jours", fmt=".0%"), 3, 30),
             ("chart", "Usage les jours favorables", total(X, M("AVG(usage_per_station)", "mouvements / station / h"),
                                                          "mouvements par station et par heure", fmt=".2f", filters=[favorable, bikes]), 3, 30),
             ("chart", "Usage les autres jours", total(X, M("AVG(usage_per_station)", "mouvements / station / h"),
                                                      "mouvements par station et par heure", fmt=".2f",
                                                      filters=[where("conditions_velo = 'Défavorable'"), bikes]), 3, 30),
             ("chart", "Jours avec données des 3 sources", total(X, M("COUNT(*)", "jours"), "jours × villes", fmt=",d",
                                                                 filters=[bikes, where("pm25 IS NOT NULL AND temperature IS NOT NULL")]), 3, 30)],
            [("md", "#### Météo → usage des vélos (mouvements par station et par heure, pour comparer des villes de tailles différentes)", 12, 6)],
            [("chart", "Usage des vélos selon la température",
              series(X, [M("AVG(usage_per_station)", "Mouvements / station / h")], x="classe_temperature", kind="bar",
                     y_fmt=".2f", filters=[bikes]), 4, 50),
             ("chart", "Usage des vélos selon la pluie du jour",
              series(X, [M("AVG(usage_per_station)", "Mouvements / station / h")], x="pluie", kind="bar", y_fmt=".2f",
                     filters=[bikes]), 4, 50),
             ("chart", "Usage des vélos selon le vent",
              series(X, [M("AVG(usage_per_station)", "Mouvements / station / h")], x="vent", kind="bar", y_fmt=".2f",
                     filters=[bikes]), 4, 50)],
            [("chart", "Usage des vélos (barres, mouvements / station / h) et température (courbe, °C)",
              mixed(X, M("AVG(usage_per_station)", "Mouvements / station / h"), M("AVG(temperature)", "Température (°C)"),
                    fmt_a=".2f"), 8, 50),
             ("chart", "Effet de la pluie heure par heure (mouvements / station / h)",
              series(UH, [M("AVG(usage_per_station)", "Mouvements / station / h")], x="pluie", groupby=["type_jour"],
                     kind="bar", y_fmt=".2f", filters=[where("usage_per_station IS NOT NULL")]), 4, 50)],
            [("md", "#### Météo → pollution", 12, 6)],
            [("chart", "PM2.5 et NO2 selon le vent (le vent disperse les polluants)",
              series(X, [M("AVG(pm25)", "PM2.5"), M("AVG(no2)", "NO2")], x="vent", groupby=[], kind="bar", y_fmt=".1f",
                     y_title="µg/m³"), 4, 50),
             ("chart", "Ozone (O3) selon la température (l'O3 se forme au soleil et à la chaleur)",
              series(X, [M("AVG(o3)", "O3")], x="classe_temperature", kind="bar", y_fmt=".1f", y_title="µg/m³"), 4, 50),
             ("chart", "PM10 selon la pluie (la pluie lave l'air)",
              series(X, [M("AVG(pm10)", "PM10")], x="pluie", kind="bar", y_fmt=".1f", y_title="µg/m³"), 4, 50)],
            [("md", "#### Synthèse et périodes de forte interaction", 12, 6)],
            [("chart", "Corrélations journalières (r de Pearson, sur toute la période) : > 0 varient ensemble, < 0 en sens inverse",
              series(COR, [M("AVG(r)", "r")], x="relation", kind="bar", y_fmt=".2f", horizontal=True,
                     filters=[], extra={"adhoc_filters": []}), 6, 60),
             ("chart", "Jours où les cyclistes sont les plus exposés (usage élevé et pollution au-dessus du seuil OMS)",
              table(X, ["timestamp", "city", "usage_per_hour", "pm25", "no2", "temperature", "precipitation"], "pm25",
                    limit=30, filters=[where("pm25 > 15 AND usage_per_hour IS NOT NULL")],
                    formats={"usage_per_hour": ",.0f", "pm25": ".1f", "no2": ".1f", "temperature": ".1f",
                             "precipitation": ".1f"}), 6, 60)],
            [("chart", "Part des jours favorables au vélo, par mois",
              own(series(X, [M("AVG(CASE WHEN conditions_velo = 'Favorable' THEN 1.0 ELSE 0 END)", "Jours favorables")],
                         kind="bar", grain="P1M", y_fmt=".0%"), grain=True), 6, 45),
             ("chart", "Profil d'une journée : vélos, NO2 et PM2.5 (1 = moyenne de la ville) · le NO2 suit les heures de pointe",
              series(UH, [M("AVG(usage_idx)", "Vélos"), M("AVG(no2_idx)", "NO2"), M("AVG(pm25_idx)", "PM2.5")],
                     x="heure", groupby=[], y_fmt=".2f"), 6, 45)],
        ]),
    ]


# ============================================================================ assemblage
def build():
    api = Superset()
    db = ensure_database(api)
    ds = {name: ensure_dataset(api, db, name) for name in PHYSICAL}
    ds.update({name: ensure_dataset(api, db, name, sql.strip()) for name, sql in DATASETS.items()})
    print(f"{len(ds)} datasets prêts")
    refs = {name: f"{i}__table" for name, i in ds.items()}

    dash = api.find("dashboard", "slug", SLUG) or api.find("dashboard", "dashboard_title", TITLE)
    dash_id = dash[0]["id"] if dash else api.call("POST", "dashboard/", json={"dashboard_title": TITLE, "slug": SLUG})["id"]
    for c in api.call("GET", f"dashboard/{dash_id}/charts")["result"]:   # anciens graphiques de ce dashboard
        api.call("DELETE", f"chart/{c['id']}")

    position = {"DASHBOARD_VERSION_KEY": "v2",
                "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS-main"]},
                "TABS-main": {"type": "TABS", "id": "TABS-main", "children": [], "parents": ["ROOT_ID"], "meta": {}},
                "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
                "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}}}
    tab_ids, charts_by_tab, checks, excluded_time, excluded_grain = {}, {}, [], [], []
    for t, (tab_name, rows) in enumerate(layout(refs)):
        tab = f"TAB-{t}"
        tab_ids[tab_name] = tab
        position["TABS-main"]["children"].append(tab)
        position[tab] = {"type": "TAB", "id": tab, "children": [], "parents": ["ROOT_ID", "TABS-main"],
                         "meta": {"text": tab_name, "defaultText": "Tab title", "placeholder": "Tab title"}}
        charts_by_tab[tab] = []
        for r, row in enumerate(rows):
            row_id = f"ROW-{t}-{r}"
            parents = ["ROOT_ID", "TABS-main", tab, row_id]
            position[tab]["children"].append(row_id)
            position[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": parents[:-1],
                                "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            for e, item in enumerate(row):
                if item[0] == "md":
                    _, text, width, height = item
                    node = f"MARKDOWN-{t}-{r}-{e}"
                    position[node] = {"type": "MARKDOWN", "id": node, "children": [], "parents": parents,
                                      "meta": {"width": width, "height": height, "code": text}}
                else:
                    _, name, params, width, height = item
                    no_time, no_grain = params.pop("_no_time_filter", False), params.pop("_no_grain_filter", False)
                    ds_id = int(params["datasource"].split("__")[0])
                    chart = api.call("POST", "chart/", json={
                        "slice_name": name, "viz_type": params["viz_type"], "datasource_id": ds_id,
                        "datasource_type": "table", "params": json.dumps(params), "dashboards": [dash_id]})
                    node = f"CHART-{chart['id']}"
                    position[node] = {"type": "CHART", "id": node, "children": [], "parents": parents,
                                      "meta": {"width": width, "height": height, "chartId": chart["id"], "sliceName": name}}
                    charts_by_tab[tab].append(chart["id"])
                    checks.append((chart["id"], name, params))
                    if no_time:
                        excluded_time.append(chart["id"])
                    if no_grain:
                        excluded_grain.append(chart["id"])
                position[row_id]["children"].append(node)
    all_charts = [c for ids in charts_by_tab.values() for c in ids]
    print(f"{len(all_charts)} graphiques créés")

    def nf(name, ftype, targets, default=None, tabs=None, control=None, description=""):
        scope_tabs = [tab_ids[x] for x in tabs] if tabs else []
        in_scope = [c for tb in (scope_tabs or charts_by_tab) for c in charts_by_tab[tb]]
        mask = {"extraFormData": {}, "filterState": {}, "ownState": {}}
        if default is not None:
            mask = default
        return {"id": f"NATIVE_FILTER-{uuid.uuid4().hex[:10]}", "name": name, "filterType": ftype,
                "targets": targets, "defaultDataMask": mask, "cascadeParentIds": [], "type": "NATIVE_FILTER",
                "scope": {"rootPath": scope_tabs or ["ROOT_ID"], "excluded": []}, "chartsInScope": in_scope,
                "tabsInScope": scope_tabs or list(tab_ids.values()), "description": description,
                "controlValues": control or {}}

    select = {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
              "searchAllOptions": False, "inverseSelection": False}
    filters = [
        nf("Période", "filter_time", [{}],
           {"extraFormData": {"time_range": "Last year"}, "filterState": {"value": "Last year"}, "ownState": {}},
           control={"enableEmptyFilter": False}, description="Dates affichées (par défaut : les 12 derniers mois)"),
        nf("Granularité", "filter_timegrain", [{"datasetId": ds["weather_city_hourly"]}],
           {"extraFormData": {"time_grain_sqla": "P1W"}, "filterState": {"value": ["P1W"]}, "ownState": {}},
           control={"enableEmptyFilter": False}, description="Pas de temps des courbes : heure, jour, semaine, mois…"),
        nf("Ville", "filter_select", [{"datasetId": ds["weather_city_hourly"], "column": {"name": "city"}}],
           control=select),
        nf("Type de jour", "filter_select", [{"datasetId": ds["v_mobility_hourly"], "column": {"name": "type_jour"}}],
           control=select, description="Semaine ou week-end"),
        nf("Polluant", "filter_select", [{"datasetId": ds["v_pollution_long"], "column": {"name": "pollutant"}}],
           {"extraFormData": {"filters": [{"col": "pollutant", "op": "IN", "val": ["PM2.5"]}]},
            "filterState": {"value": ["PM2.5"], "label": "PM2.5"}, "ownState": {}},
           tabs=["Pollution"], control={**select, "multiSelect": False, "enableEmptyFilter": True},
           description="Polluant affiché dans l'onglet Pollution"),
    ]
    # graphiques qui gardent leurs propres réglages : « tous polluants », période ou pas de temps fixes
    all_pollutants = [cid for cid, name, _ in checks if name.startswith(("Indice OMS moyen par polluant", "Jours au-dessus du seuil OMS par polluant"))]
    for f, excluded in ((filters[0], excluded_time), (filters[1], excluded_grain), (filters[-1], all_pollutants)):
        f["scope"]["excluded"] = excluded
        f["chartsInScope"] = [c for c in f["chartsInScope"] if c not in excluded]

    metadata = {
        "native_filter_configuration": filters, "color_scheme": "supersetColors", "label_colors": LABEL_COLORS,
        "shared_label_colors": [], "color_scheme_domain": SCHEME_COLORS, "map_label_colors": {},
        "cross_filters_enabled": True,
        "refresh_frequency": 3600, "timed_refresh_immune_slices": [], "expanded_slices": {}, "default_filters": "{}",
        "chart_configuration": {str(c): {"id": c, "crossFilters": {"scope": "global", "chartsInScope": [x for x in all_charts if x != c]}}
                                for c in all_charts},
        "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": all_charts},
        "positions": position,
    }
    api.call("PUT", f"dashboard/{dash_id}", json={
        "dashboard_title": TITLE, "slug": SLUG, "published": True,
        "position_json": json.dumps(position), "json_metadata": json.dumps(metadata),
        "css": ".dashboard-markdown h2, .dashboard-markdown h3 { margin-top: 0; }"})
    print(f"Dashboard prêt : {os.environ.get('SUPERSET_PUBLIC_URL', 'http://localhost:8088')}/superset/dashboard/{SLUG}/")
    return api, checks


# ============================================================================ vérification
def query_for(params: dict) -> dict:
    """Requête équivalente au graphique, pour vérifier qu'elle s'exécute (sans post-traitement)."""
    cols, metrics = [], []
    x = params.get("x_axis")
    if x == "timestamp":
        cols.append({"columnType": "BASE_AXIS", "sqlExpression": "timestamp", "label": "timestamp",
                     "expressionType": "SQL", "timeGrain": params.get("time_grain_sqla", "P1D")})
    elif x:
        cols.append(x)
    for key in ("groupby", "all_columns"):
        val = params.get(key) or []
        cols += [val] if isinstance(val, str) else list(val)
    for key in ("entity", "series"):
        if params.get(key):
            cols.append(params[key])
    for key in ("metrics", "metrics_b"):
        metrics += params.get(key) or []
    for key in ("metric", "x", "y", "size"):
        if isinstance(params.get(key), dict):
            metrics.append(params[key])
    filters = [{"col": f["subject"], "op": "TEMPORAL_RANGE", "val": "Last year"}
               for f in params.get("adhoc_filters", []) if f.get("operator") == "TEMPORAL_RANGE"]
    where_sql = " AND ".join(f"({f['sqlExpression']})" for f in params.get("adhoc_filters", [])
                             if f.get("expressionType") == "SQL")
    q = {"columns": cols, "metrics": metrics, "filters": filters, "row_limit": 5, "extras": {"where": where_sql}}
    if params.get("query_mode") == "raw":
        q["metrics"] = None   # mode « lignes brutes » : pas de GROUP BY
        q["orderby"] = [json.loads(o) for o in params.get("order_by_cols", [])]
    ds_id = int(params["datasource"].split("__")[0])
    return {"datasource": {"id": ds_id, "type": "table"}, "queries": [q], "result_format": "json", "result_type": "full"}


def verify(api: Superset, checks: list) -> int:
    errors = 0
    for cid, name, params in checks:
        try:
            res = api.call("POST", "chart/data", json=query_for(params))["result"][0]
            n = res.get("rowcount", 0)
            status = "OK " if n else "VIDE"
            if not n:
                errors += 0   # vide = pas d'erreur SQL (ex. aucun jour anormal sur la période)
            print(f"  {status} {cid:4d} {name[:90]} ({n} lignes)")
        except Exception as e:  # noqa: BLE001
            errors += 1
            print(f"  ERR  {cid:4d} {name[:90]}\n       {str(e)[:400]}")
    return errors


if __name__ == "__main__":
    api, checks = build()
    print("Vérification des requêtes :")
    sys.exit(1 if verify(api, checks) else 0)
