# DGLH — Lakehouse Iceberg DISTRIBUÉ · jeu de données « grande surface tunisienne »

> **Version distribuée** : voir [`README_DISTRIBUE.md`](README_DISTRIBUE.md) (MinIO 4 nœuds, cluster Spark, génération en 4 shards). `docker-compose.mono.yml` = version mono-nœud.

**Phase 1 (nouvelle)** : génération du RAW synthétique → dossier [`generateur/`](generateur/README.md)
(`cd generateur && python run_all.py && python validate.py`). L'infra Docker (MinIO, PostgreSQL, Lakekeeper, Spark, Jupyter)
est conservée telle quelle ; `./generateur` est monté dans Jupyter et la sortie arrive dans `./data/raw`.
**Phases suivantes** : ingestion raw → bronze → silver → gold (partition bronze par `magasin_id`, tri par date, Parquet ~128 Mo).

Les notebooks `01…08` et `scripts/generate_data.py` ci-dessous sont le prototype **Smart Library** d'origine (conservé pour référence, non modifié).

---

# Smart Library — Data Lakehouse Platform (Apache Iceberg)

Plateforme intelligente de gestion de bibliothèque démontrant une architecture
**Data Lakehouse complète** avec Medallion Architecture (Bronze → Silver → Gold),
basée sur **Apache Iceberg** et le catalogue REST **Lakekeeper**.

> Version Apache Hudi → migrée vers Apache Iceberg (catalogue REST Lakekeeper +
> PostgreSQL pour les métadonnées, MinIO pour le stockage objet).

## Stack Technique

| Composant | Rôle | Port hôte |
|-----------|------|-----------|
| **MinIO** | Data Lake (stockage objet S3-compatible) | 9010 / 9011 |
| **PostgreSQL** | Backend du catalogue Lakekeeper (métadonnées) | 5433 |
| **Lakekeeper** | Catalogue REST Apache Iceberg | 8182 |
| **Apache Iceberg** | Couche Lakehouse (UPSERT via MERGE INTO, Time Travel, ACID) | — |
| **Apache Spark 3.4** | Moteur de traitement batch + streaming | — |
| **JupyterLab** | Environnement de développement | 8889 |

> **Isolation par rapport à un autre projet lakehouse sur la même machine** :
> ports hôte, noms de conteneurs (`icebergv2-*`), network (`icebergv2-net`) et
> volumes (`icebergv2-*`) sont tous distincts du projet d'origine. Les deux
> stacks peuvent tourner **en même temps** sans conflit — voir
> [Faire tourner deux projets en parallèle](#faire-tourner-deux-projets-lakehouse-en-parallèle)
> plus bas.

## Démarrage rapide

### 1. Prérequis
```bash
docker --version    # >= 20.x
docker compose version  # >= 2.x
# RAM recommandée : 8 Go minimum
```

### 2. Lancer la stack
```bash
git clone <repo>
cd smart-library-lakehouse
docker compose up -d
```

Au premier démarrage, les services `minio-init` et `lakekeeper-init` créent
automatiquement le bucket `lakehouse` et le warehouse Lakekeeper correspondant.

### 3. Vérifier que tout tourne
```bash
docker compose ps
# Tous les services doivent être "healthy" ou "running"
```

### 4. Interfaces disponibles
- **JupyterLab** → http://localhost:8889 (token : `smartlibrary2024v2`)
- **MinIO Console** → http://localhost:9011 (minioadmin / minioadmin123)
- **Lakekeeper (catalogue REST)** → http://localhost:8182

### 5. Générer les données
Dans JupyterLab, ouvrir un terminal :
```bash
cd /home/jovyan
python scripts/generate_data.py
```

### 6. Exécuter les notebooks dans l'ordre
```
01_ingestion_bronze.ipynb     ← Ingestion CSV/JSON/XML → Iceberg Bronze
02_traitement_silver.ipynb    ← Nettoyage + UPSERT (MERGE INTO) → Iceberg Silver
03_analytique_gold.ipynb      ← Agrégations business + Time Travel → Iceberg Gold
04_streaming_pipeline.ipynb   ← Micro-batches → Iceberg merge-on-read
05_time_travel_demo.ipynb     ← Time Travel avancé + Changelog + Audit
06_recommandations.ipynb      ← Système de recommandation KNN/ALS
07_dashboard_reel.ipynb       ← Dashboard HTML à partir des vraies données
08_enrichissement_metadonnees.ipynb  ← Data catalog enrichi via LLM (Groq)
```

> `08_enrichissement_metadonnees.ipynb` nécessite une clé API Groq gratuite
> ([console.groq.com/keys](https://console.groq.com/keys)), à définir dans un
> fichier `.env` à la racine : `GROQ_API_KEY=gsk_xxx` (non commité).

## Architecture Medallion

```
Sources                 MinIO (Data Lake)              Iceberg (Lakehouse)
───────                 ─────────────────              ────────────────────
CSV (users)    ──┐
JSON (books)   ──┤──► s3a://lakehouse/bronze/  ──►  Tables Iceberg copy-on-write
XML (catalogue)──┤         (données brutes)          (partitionnées)
CSV (orders)   ──┘                                   catalogue REST : Lakekeeper
                                  │
                                  ▼
                     s3a://lakehouse/silver/   ──►  Tables Iceberg copy-on-write
                      (nettoyées, enrichies)         (UPSERT via MERGE INTO)
                                  │
                                  ▼
 (events) ──►   s3a://lakehouse/gold/    ──►  Tables Iceberg merge-on-read
  clics, search        (analytics, ML-ready)         (streaming)
  panier, avis                    │
                                  ▼
                          Dashboard HTML (Chart.js)
```

Le catalogue **Lakekeeper** (backend PostgreSQL) centralise toutes les métadonnées
Iceberg — schémas, partitions, snapshots — indépendamment du moteur de calcul
(Spark ici, mais Trino/Flink/DuckDB pourraient s'y connecter de la même façon).

## Cas d'usage démontrés

| Cas d'usage | Technologie Iceberg | Notebook |
|-------------|----------------------|----------|
| Mise à jour stock temps réel | `MERGE INTO` (UPSERT) | 02, 04 |
| Top livres les plus vendus | Spark SQL + Gold | 03 |
| Historique des ventes | Time Travel (`snapshot-id` / `VERSION AS OF`) | 03, 05 |
| Analyse comportement user | Spark ML + Gold (RFM) | 03 |
| Recommandation personnalisée | KNN + ALS | 06 |
| Tendances lecture temps réel | Micro-batches + merge-on-read | 04 |
| Requêtes de changements | `create_changelog_view` (changelog) | 02, 05 |
| Comparaison COW / MOR | `write.update.mode` / `write.merge.mode` | 01 |
| Data catalog enrichi (descriptions auto) | `ALTER TABLE ... COMMENT` + LLM (Groq) | 08 |

## Structure du projet

```
smart-library-lakehouse/
├── docker-compose.yml          ← Stack complète (MinIO, Postgres, Lakekeeper, Spark)
├── README.md
├── config/
│   └── config.py               ← Chemins, tables, helpers Iceberg centralisés
├── spark-conf/
│   └── spark-defaults.conf     ← Config Spark/Iceberg/Lakekeeper/MinIO
├── data/
│   ├── batch/                  ← Données générées (CSV, JSON, XML, PDF)
│   └── streaming/              ←
├── notebooks/
│   ├── 01_ingestion_bronze.ipynb
│   ├── 02_traitement_silver.ipynb
│   ├── 03_analytique_gold.ipynb
│   ├── 04_streaming_pipeline.ipynb
│   ├── 05_time_travel_demo.ipynb
│   ├── 06_recommandations.ipynb
│   ├── 07_dashboard_reel.ipynb
│   └── 08_enrichissement_metadonnees.ipynb
├── scripts/
│   └── generate_data.py        ← Générateur données batch

```

## Faire tourner deux projets lakehouse en parallèle

Si tu as déjà un autre projet similaire (ex: la version Hudi, ou un autre labo
Iceberg) sur la même machine, deux catégories de choses peuvent entrer en
conflit quand les deux stacks tournent **en même temps** :

| Élément | Risque de conflit | Statut dans ce projet |
|---|---|---|
| **Ports hôte** (le chiffre à gauche du `:` dans `"9010:9000"`) | Bloquant — deux process ne peuvent pas écouter sur le même port de ta machine | ✅ Décalés (9010/9011, 5433, 8182, 8889) |
| **`container_name`** | Bloquant — Docker refuse deux conteneurs du même nom, peu importe le projet | ✅ Préfixés `icebergv2-*` |
| **Network** (`icebergv2-net`) | Rare (Compose préfixe déjà par nom de dossier) | ✅ Renommé quand même, par sécurité |
| **Volumes** (`icebergv2-minio-data`, `icebergv2-pg-data`) | Rare, même raison | ✅ Renommés quand même |

**Ce qui NE change PAS et n'a pas besoin de changer** : les adresses internes
entre conteneurs (`http://minio:9000`, `http://lakekeeper:8181`,
`postgres:5432` dans `config.py`, les notebooks, `spark-defaults.conf`...).
Ce sont des noms/ports **internes au network Docker** de ce projet — ils ne
sortent jamais vers ta machine, donc aucune collision possible avec l'autre
projet même s'il utilise les mêmes noms de service en interne.

### Pour vérifier qu'il n'y a plus aucun conflit
```bash
# 1. Lance d'abord l'autre projet, puis celui-ci
docker compose up -d

# 2. Si une erreur "port is already allocated" ou "container name already
#    in use" apparaît, note le port/nom concerné et ajuste-le dans ce fichier
docker compose ps
```

### Pour aller plus loin (optionnel)
Tu peux aussi fixer un nom de projet explicite pour plus de clarté dans
`docker compose ps` / `docker ps` :
```bash
# Dans un fichier .env à la racine, en plus de GROQ_API_KEY :
COMPOSE_PROJECT_NAME=smartlibrary-iceberg-v2
```

## Commandes utiles

```bash
# Suivre les logs d'un service
docker compose logs -f jupyter

# Suivre les logs du catalogue Lakekeeper
docker compose logs -f lakekeeper

# Redémarrer un service
docker compose restart jupyter

# Arrêter tout
docker compose down

# Arrêter et supprimer les volumes (reset complet — recrée le catalogue à vide)
docker compose down -v
```

## Notes pour la thèse — Différences clés Hudi → Iceberg

| Concept | Apache Hudi | Apache Iceberg |
|---|---|---|
| Catalogue | Intégré à Spark (`HoodieCatalog`) | REST externe (Lakekeeper), découplé du moteur |
| UPSERT | Automatique (`mode("append")` + `recordkey.field`) | Explicite via `MERGE INTO` |
| COW / MOR | Type de table déclaré à l'écriture | Propriété de table (`write.*.mode`) |
| Time Travel | `as.of.instant` + `_hoodie_commit_time` | `snapshot-id` / `VERSION AS OF`, metadata tables |
| Colonnes techniques | `_hoodie_*` mêlées aux données | Aucune — métadonnées 100% côté catalogue |
| Requête incrémentale | Filtre SQL sur `_hoodie_commit_time` | `create_changelog_view` (procédure système) |

Ce projet démontre concrètement les concepts suivants :
- **Unification batch + streaming** dans une même architecture (Lambda → Kappa simplifié)
- **Gestion des mises à jour** sans réécriture complète (`MERGE INTO` vs overwrite)
- **Traçabilité des données** via les snapshots Iceberg (audit, time travel)
- **Gouvernance des données** via un catalogue REST centralisé (Lakekeeper), partitionnement,
  schéma évolutif et métadonnées
- **Valeur business** via analytics et système de recommandation
