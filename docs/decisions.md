# Décisions d'équipe

Toute décision qui touche les trois développeurs (contrat de données, villes, période,
stack) est notée ici avec sa date.

## À valider à la réunion de lancement

Propositions par défaut du guide (section 8). Remplacer « à valider » par la décision prise.

| Sujet | Proposition | Statut |
|---|---|---|
| Villes | paris, lyon, lille | décidé le 2026-11-19 |
| Période | 2024 → 2026 (année en cours mise à jour en fin de projet) | décidé le 2026-11-19 |
| Machine de collecte 24 h/24 | PC toujours allumé ou petite VM gratuite | à valider |
| Fréquences | CityBikes 2 min · OpenAQ 15 min · jobs Spark toutes les heures | à valider |
| Assemblage du dashboard | à désigner | à valider |
| Date de rendu | à reporter | à valider |

## Décisions prises

| Date | Décision | Qui |
|---|---|---|
| 2026-11-19 | Structure initiale du dépôt créée (dossiers, `.gitignore`, `.env.example`, `requirements.txt`, `CLAUDE.md`) | Dev 1 |
| 2026-11-19 | Villes réduites à paris, lyon, lille | Dev 3 (à valider par Dev 1 et Dev 2 dans la PR) |
| 2026-11-19 | Période étudiée : 2024, 2025, 2026 (toutes sources) | Dev 3 (à valider par Dev 1 et Dev 2 dans la PR) |
| 2026-11-19 | OpenAQ : seules les stations officielles fixes sont gardées (`isMonitor = true`, réseau EEA) ; capteurs bon marché (AirGradient, HabitatMap) écartés | Dev 3 |
