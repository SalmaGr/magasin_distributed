# Générateur RAW — grande surface tunisienne (style Aziza)

Jeu de données synthétique (~40 Go, 20 magasins, 24 mois) pour tester la gouvernance / l'enrichissement métier du DGLH
sur un Lakehouse Iceberg. Python 3.11+, Polars, NumPy, PyYAML.

## Installation
```bash
pip install polars numpy pyyaml          # déjà installé dans le conteneur Jupyter du docker-compose
cd generateur
```

## Utilisation
```bash
python run_all.py                  # 1) ÉCHANTILLON (config.yaml : mode: sample) : 1 mois, 3 magasins (hyper, super, proximité)
python validate.py                 # contrôle des critères d'acceptation (33 contrôles)
python run_all.py --estimate       # 2) extrapolation du volume complet (≈ 15 s)
python run_all.py --mode full      # 3) génération complète (reprenable : relancer ignore les fichiers existants)
python validate.py --mode full --ratio 0.03    # validation sur 3 % des (magasin, jour)
```
Options : `--workers 1` (séquentiel, débogage) · `--skip-ref` (réutilise magasins/produits/clients déjà générés) ·
`--only ventes,avis` · `--out <dossier>`. Tout est piloté par `config.yaml` (mode, période, magasins, volumes, défauts, compression `gz`).
Sorties : `../data/raw` (full) ou `../data/raw_echantillon` (sample) ; vérité terrain et journaux dans `_meta/`.

## Garanties
- **Reproductible** : une graine par (magasin, jour) → mêmes fichiers quel que soit le nombre de workers (vérifié : workers 1 vs 2, diff vide).
- **Idempotent / reprenable** : écriture atomique (tmp → rename), un fichier existant est ignoré ; le journal des anomalies survit aux reprises.
- **Multiprocessing `spawn`** (jamais `fork`), tâche = (magasin, mois), mémoire bornée (écriture par fichier-jour).

## Modules
| fichier | rôle |
|---|---|
| `config.yaml` | tous les paramètres |
| `referentiels.py` | géographie, noms, catalogue produits, calendrier (Ramadan, Aïd, rentrée, soldes, été, week-end) |
| `common.py` | RNG, écriture atomique, formats tunisiens (+216, CIN, RIB mod 97, MF, EAN 619), injection de défauts, PDF minimal |
| `src_ref.py` | magasins (2 snapshots), employés RH |
| `src_produits.py` | 9 tables produits (7 catégories + `prod_x1`, `art_divers`), prix mensuels (inflation par paliers), promotions, exports `export_AAAA-MM-01.csv` |
| `src_fournisseurs.py` | fiches (2 snapshots), tarifs mensuels |
| `src_clients.py` | CRM CSV, carte de fidélité CSV, comptes web JSONL ; doublons inter/intra-sources, ~3 % de mises à jour |
| `src_ventes.py` | tickets / lignes / paiements (1 fichier par magasin et par jour), stock, logs POS JSONL, estimation |
| `src_ecommerce.py` | commandes, clickstream, catalogue API |
| `src_avis.py` | avis réseaux sociaux, formulaire site, emails, transcriptions d'appels |
| `src_documents.py` | factures XML, contrats PDF, fiches techniques PDF |
| `run_all.py` / `validate.py` | orchestration (`--estimate`, `--skip-ref`) / contrôles |

## Volume (estimation `--estimate`, config par défaut)
≈ 38,5 Go (cible 40 ±10 %) : ventes structurées 14,8 · logs POS 11,0 · e-commerce 6,6 · texte libre 2,9 · XML/PDF 3,1 · référentiels 0,2.
~29 M tickets, ~170 M lignes. Durée indicative : ventes+logs ≈ 6 min avec 4 workers (mesuré : 3 magasins × 31 jours = 14 s sur 1 cœur) ;
l'ensemble de la génération complète reste de l'ordre d'une à quelques dizaines de minutes selon les cœurs. Disque : ~40 Go de données sur les 120 Go prévus.
Réglages pour recaler le volume : `pos_logs.detail_ratio`, `ecommerce.evenements_jour`, `avis.*`, `documents.factures_jour`.

## Écarts / choix à connaître
- `tk_id` est une chaîne `TK-magasin_01-20250314-000123` (clé unique inter-magasins).
- Défauts : « aberrants » = taux global réparti sur `pu_ht` et `qte` ; les doublons exacts sont insérés juste après l'original ; les validations dédoublonnent avant de comparer.
- En échantillon, les dates d'évolution de schéma sont ramenées dans la fenêtre (`echantillon.evolutions`) pour être testables.
- Le stock (`erp/stock/...`) et `verite_clients.csv` sont des ajouts ; `_meta/` contient aussi `verite_terrain.json`, `journal_anomalies.csv`, `rapport_generation.json`.
- Mises à jour clients : chaque mise à jour tire de nouvelles valeurs (l'état « courant » n'est pas chaîné d'un mois à l'autre).
