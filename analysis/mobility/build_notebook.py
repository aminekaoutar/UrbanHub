"""Génère partie2_mobilite.ipynb (python analysis/mobility/build_notebook.py), puis l'exécuter avec jupyter nbconvert."""
from pathlib import Path

import nbformat as nbf

cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip()))
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip()))

md("""
# UrbanHub · Partie 2 — Mobilité urbaine (CityBikes)

**Développeur 2** · Sources : `clean/citybikes/station_status` et `curated/citybikes/station_hourly`, produites par
`pipelines/streaming_citybikes/` à partir du collecteur (relevé toutes les 3 min, 9 villes).

| # | Question métier | Indicateur |
|---|---|---|
| 1 | Stations au plus fort taux d'utilisation | rotation = mouvements de vélos / capacité |
| 2 | Zones où l'offre est insuffisante | % du temps où les stations d'une zone sont vides |
| 3 | Pics d'utilisation journaliers | mouvements par heure de la journée (heure de Paris) |
| 4 | Déséquilibres géographiques | taux de remplissage selon la distance au centre |
| 5 | Stations critiques à rééquilibrer | stations vides (< 10 %) ou pleines (> 90 %) plus de 50 % du temps |

> **Mouvements de vélos** (`usage_events`) = somme des variations absolues de `bikes_available` entre deux relevés
> successifs (mesure commune du contrat). C'est une borne basse : un vélo pris puis rendu dans les 3 min est invisible.
""")

code("""
import sys
from pathlib import Path
ROOT = Path.cwd().resolve()
while not (ROOT / "common").exists():
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from common import config

sns.set_theme(style="whitegrid")
FIG = ROOT / "docs" / "resultats" / "figures" / "mobilite"
FIG.mkdir(parents=True, exist_ok=True)
TZ = "Europe/Paris"

def lire(couche, chemin):
    return pd.read_parquet(config.lake_uri(couche, chemin))

st = lire("curated", "citybikes/station_hourly")
st["timestamp"] = pd.to_datetime(st["timestamp"]).dt.tz_localize("UTC")
st["heure_locale"] = st["timestamp"].dt.tz_convert(TZ)
st["heure"] = st["heure_locale"].dt.hour
for c in ("is_empty", "is_full"):                      # null quand la capacité est inconnue -> NaN
    st[c] = st[c].astype("float")
st["jour"] = np.where(st["heure_locale"].dt.dayofweek >= 5, "week-end", "semaine")

# Marseille : le flux renvoie 0 vélo dans toutes les stations (problème de la source, voir docs) -> exclue des analyses
st = st[st["city"] != "marseille"]

periode = f"{st.heure_locale.min():%d/%m/%Y %H:%M} → {st.heure_locale.max():%d/%m/%Y %H:%M} (heure de Paris)"
print("Période analysée :", periode)
print(f"{st.station_id.nunique():,} stations · {st.city.nunique()} villes · {len(st):,} lignes station × heure")
st.groupby("city").agg(stations=("station_id", "nunique"), heures=("timestamp", "nunique"),
                       mouvements=("usage_events", "sum")).sort_values("stations", ascending=False)
""")

md("""
## Q1 · Quelles stations ont le plus fort taux d'utilisation ?

**Rotation** = mouvements de vélos par heure, rapportés à la capacité de la station (une petite station très active
compte autant qu'une grande). On ne garde que les stations observées au moins la moitié des heures.
""")
code("""
par_station = st.groupby(["station_id", "station_name", "city"]).agg(
    mouvements=("usage_events", "sum"), heures=("timestamp", "nunique"), capacite=("capacity", "median"),
    lat=("latitude", "first"), lon=("longitude", "first")).reset_index()
par_station = par_station[(par_station.capacite > 0) & (par_station.heures >= 0.5 * st.timestamp.nunique())]
par_station["mouvements_h"] = par_station.mouvements / par_station.heures
par_station["rotation_h"] = par_station.mouvements_h / par_station.capacite

top = par_station.nlargest(15, "rotation_h")
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=top, y=top.station_name + " (" + top.city + ")", x="rotation_h", hue="city", dodge=False, ax=ax)
ax.set(title="Top 15 stations : mouvements par heure / capacité", xlabel="rotation par heure", ylabel="")
plt.tight_layout(); plt.savefig(FIG / "q1_top_stations.png", dpi=120); plt.show()
top[["city", "station_name", "capacite", "mouvements_h", "rotation_h"]].round(3)
""")

md("""
## Q2 · Zones où l'offre de vélos est insuffisante

On découpe chaque ville en carreaux d'environ 1 km. Une zone est **sous-équipée** quand ses stations sont souvent
vides (< 10 % de vélos) : les usagers y trouvent rarement un vélo.
""")
code("""
CARREAU = 0.01   # ~1,1 km en latitude, ~0,75 km en longitude
z = st.assign(zone_lat=(st.latitude / CARREAU).round() * CARREAU, zone_lon=(st.longitude / CARREAU).round() * CARREAU)
zones = z.groupby(["city", "zone_lat", "zone_lon"]).agg(
    stations=("station_id", "nunique"), pct_vide=("is_empty", "mean"),
    velos_moyens=("bikes_available", "mean")).reset_index()
zones = zones[zones.stations >= 2]
zones["pct_vide"] = (zones.pct_vide * 100).round(1)

print("Zones les plus souvent vides (au moins 2 stations) :")
display(zones.nlargest(12, "pct_vide"))

villes = zones.groupby("city").apply(lambda g: (g.pct_vide >= 50).mean() * 100, include_groups=False).rename("% zones vides > 50 % du temps")
villes.sort_values(ascending=False).round(1).to_frame()
""")
code("""
paris = zones[zones.city == "paris"]
fig, ax = plt.subplots(figsize=(8, 7))
sc = ax.scatter(paris.zone_lon, paris.zone_lat, c=paris.pct_vide, s=paris.stations * 25, cmap="Reds", vmin=0, vmax=100, edgecolor="grey")
plt.colorbar(sc, label="% du temps où les stations sont vides")
ax.set(title="Paris : zones de ~1 km (taille = nombre de stations)", xlabel="longitude", ylabel="latitude")
plt.tight_layout(); plt.savefig(FIG / "q2_zones_paris.png", dpi=120); plt.show()
""")

md("## Q3 · Pics d'utilisation journaliers")
code("""
profil = (st.groupby(["city", "jour", "heure", st.heure_locale.dt.date])["usage_events"].sum()
            .groupby(["city", "jour", "heure"]).mean().reset_index(name="mouvements"))
fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)
for ax, j in zip(axes, ["semaine", "week-end"]):
    sns.lineplot(data=profil[profil.jour == j], x="heure", y="mouvements", hue="city", marker="o", ax=ax)
    ax.set(title=f"Mouvements de vélos par heure — {j}", xlabel="heure (Paris)", ylabel="mouvements / heure", xticks=range(0, 24, 2))
plt.tight_layout(); plt.savefig(FIG / "q3_pics_journaliers.png", dpi=120); plt.show()

pics = profil.loc[profil.groupby(["city", "jour"]).mouvements.idxmax(), ["city", "jour", "heure", "mouvements"]]
pics.rename(columns={"heure": "heure du pic"}).round(0)
""")

md("""
## Q4 · Déséquilibres géographiques de la disponibilité

Taux de remplissage moyen des stations selon leur **distance au centre-ville**. Un écart fort centre / périphérie
signale des flux pendulaires : les vélos s'accumulent d'un côté et manquent de l'autre.
""")
code("""
from pipelines.streaming_citybikes.city import CITIES

def km(lat1, lon1, lat2, lon2):
    p = np.radians
    a = np.sin(p(lat2 - lat1) / 2) ** 2 + np.cos(p(lat1)) * np.cos(p(lat2)) * np.sin(p(lon2 - lon1) / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(a))

occ = st.groupby(["station_id", "city"]).agg(lat=("latitude", "first"), lon=("longitude", "first"),
                                             remplissage=("occupancy_rate", "mean")).reset_index().dropna()
occ["dist_km"] = [km(a, b, *CITIES[c]) for a, b, c in zip(occ.lat, occ.lon, occ.city)]
occ["anneau"] = pd.cut(occ.dist_km, [0, 2, 5, 10, 30], labels=["< 2 km", "2-5 km", "5-10 km", "> 10 km"])

tab = occ.pivot_table(index="city", columns="anneau", values="remplissage", aggfunc="mean", observed=False).mul(100).round(1)
tab["écart-type entre stations (pts)"] = occ.groupby("city").remplissage.std().mul(100).round(1)
display(tab)

fig, ax = plt.subplots(figsize=(8, 7))
p = occ[occ.city == "paris"]
sc = ax.scatter(p.lon, p.lat, c=p.remplissage, cmap="RdYlGn", vmin=0, vmax=1, s=12)
plt.colorbar(sc, label="taux de remplissage moyen")
ax.set(title="Paris : remplissage moyen par station", xlabel="longitude", ylabel="latitude")
plt.tight_layout(); plt.savefig(FIG / "q4_desequilibre_paris.png", dpi=120); plt.show()
""")

md("""
## Q5 · Stations critiques à rééquilibrer

Une station est **critique** quand elle est **vide** (< 10 % de vélos : on ne peut plus emprunter) ou **pleine**
(> 90 % : on ne peut plus rendre) pendant **plus de 50 %** des heures observées.
""")
code("""
crit = st.groupby(["station_id", "station_name", "city"]).agg(
    pct_vide=("is_empty", "mean"), pct_pleine=("is_full", "mean"), capacite=("capacity", "median")).reset_index()
crit = crit[crit.capacite > 0]
crit["statut"] = np.select([crit.pct_vide > 0.5, crit.pct_pleine > 0.5], ["à remplir (souvent vide)", "à vider (souvent pleine)"], "ok")

resume = crit.groupby(["city", "statut"]).size().unstack(fill_value=0)
resume["% critiques"] = (100 * (1 - resume.get("ok", 0) / resume.sum(axis=1))).round(1)
display(resume.sort_values("% critiques", ascending=False))

liste = crit[crit.statut != "ok"].assign(pct_vide=lambda d: (d.pct_vide * 100).round(0), pct_pleine=lambda d: (d.pct_pleine * 100).round(0))
liste.sort_values(["pct_vide", "pct_pleine"], ascending=False).head(20)
""")
code("""
ax = resume.drop(columns="% critiques").drop(columns="ok", errors="ignore").plot.bar(stacked=True, figsize=(10, 5), color=["#d9534f", "#5b8def"])
ax.set(title="Stations critiques par ville", ylabel="nombre de stations", xlabel="")
plt.tight_layout(); plt.savefig(FIG / "q5_stations_critiques.png", dpi=120); plt.show()
""")

md("""
## Limites

- Les résultats dépendent de la durée de collecte : il faut **au moins 2 semaines** pour comparer semaine / week-end
  et lisser la météo. Relancer ce notebook après `clean` + `curate`.
- `usage_events` sous-estime l'usage réel (mouvements compensés entre deux relevés de 3 min).
- **Nice** n'a pas de réseau à stations dans CityBikes ; **Marseille** renvoie 0 vélo partout (problème de la source) : exclues.
""")

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, Path(__file__).with_name("partie2_mobilite.ipynb"))
print("partie2_mobilite.ipynb généré")
