# DGLH — version distribuée (données + calcul), simulée sur 1 machine avec Docker

```
                 ┌──────────── 4 générateurs (profil "gen") ────────────┐
                 │ gen-0 (+référentiels)  gen-1   gen-2   gen-3         │   tâche k -> shard k % 4
                 └───────────────┬──────────────────────────────────────┘
                                 │ S3 (boto3)
                          ┌──────▼──────┐   http://minio:9000
                          │  nginx (LB) │
                          └──┬──┬──┬──┬─┘
                  minio1  minio2  minio3  minio4      ← MinIO DISTRIBUÉ (erasure coding, tolère 2 nœuds en panne)
                  buckets : raw · raw-echantillon · lakehouse
                                 │ s3a://
   Jupyter (DRIVER) ──► spark-master ──► spark-worker-1/2/3        ← CALCUL DISTRIBUÉ
        │                                      │ écriture Iceberg
        └────── Lakekeeper (REST) + PostgreSQL ┘   tables lakehouse.bronze.*
```

## Démarrage
```bash
cp .env.example .env                                   # ressources (RAM/CPU) et mode : GEN_MODE=sample|full
docker compose up -d                                   # MinIO x4 + LB, Lakekeeper, Postgres, Spark master + 3 workers, Jupyter
docker compose --profile gen up --build                # 4 conteneurs générateurs ; le shard 0 produit d'abord les référentiels
docker compose --profile finalize run --rm gen-finalize    # contrôle 17/17 tâches, journal des anomalies, rapport, vérité terrain
docker compose exec jupyter spark-submit /home/jovyan/spark_jobs/ingestion_bronze.py --bucket raw-echantillon --compaction
```
En mode `full` : `GEN_MODE=full` dans `.env`, puis `--bucket raw`. Interfaces : MinIO http://localhost:9001 · Spark master http://localhost:8080 ·
job en cours http://localhost:4040 · Jupyter http://localhost:8889 (token `dglh2024`) · Lakekeeper http://localhost:8182.

## Ce qui est distribué
| couche | comment |
|---|---|
| **Stockage** | MinIO 4 nœuds (1 volume chacun) derrière nginx ; URL inchangée `http://minio:9000`. Erasure coding : ~50 % de capacité utile (40 Go de raw ≈ 80 Go de disque). |
| **Génération** | `generateur/distribue.py` : tâche = (magasin, mois) ou mois ; tâche k -> shard k % N. Chaque tâche est générée dans un dossier temporaire, téléversée puis supprimée (disque borné) ; marqueur `_meta/_done/…` posé **après** le téléversement => reprise sûre. |
| **Calcul** | `spark_jobs/ingestion_bronze.py` s'exécute sur le cluster : lecture parallèle des fichiers `s3a://`, écriture Iceberg partitionnée par `magasin_id`, tri par `dt_fichier`, fichiers ≈ 128 Mo, `ALTER TABLE ADD COLUMN` pour l'évolution de schéma, reprise par lot (`_mois`), compaction + expiration de snapshots (`--compaction`). |

## Démonstration de tolérance de panne
`docker stop dglh-minio3` pendant la génération ou l'ingestion : lectures et écritures continuent (EC:2). `docker start dglh-minio3` : la guérison est automatique.
Idem pour la génération : `docker stop dglh-gen-2` puis relancer `docker compose --profile gen up gen-2` reprend là où il s'est arrêté.

## Ressources conseillées (machine unique)
≥ 16 Go de RAM, 6+ cœurs. Répartition par défaut : 3 workers Spark × (2 cœurs, 3 Go), driver 2 Go, générateurs 4 × (1,5 cœur, 3 Go).
Lance les phases l'une après l'autre (génération, puis Spark) ; réduis `GEN_MEM` / `SPARK_WORKER_MEMORY` si besoin. Disque : prévoir ~110 Go pour le mode full.

## Ce qui a été vérifié, et ce qui ne l'a pas été
- **Vérifié** (sandbox, sans Docker) : répartition des tâches par shard ; génération à 2 shards **identique octet pour octet** au mode mono-nœud ;
  reprise (tâches « déjà fait ») ; téléversement via le protocole S3 contre un émulateur (6 009 objets) ; `finalize` détecte les tâches manquantes ;
  `validate.py` 33/33 sur la copie ; syntaxe du job Spark et regroupement par mois ; YAML du compose valide.
- **Non vérifié** : le démarrage réel des conteneurs (MinIO distribué, Lakekeeper, cluster Spark) et l'exécution du job Spark — pas de Docker ni de Spark dans mon environnement.
  Points à surveiller au premier lancement : santé des 4 nœuds MinIO avant `minio-init` ; accès du driver (conteneur `jupyter`) depuis les workers ;
  droits STS/remote-signing de Lakekeeper avec le répartiteur nginx. Si un conteneur échoue, envoie-moi `docker compose logs <service>`.
- **Limite connue** : le job Spark lit un mois à la fois avec l'en-tête du premier fichier ; les évolutions de schéma doivent donc tomber sur une frontière de mois (c'est le cas dans le `config.yaml` fourni).

Les anciens notebooks 01–08 (Smart Library) et `docker-compose.mono.yml` (version mono-nœud) sont conservés tels quels.
