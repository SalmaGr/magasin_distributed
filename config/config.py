# ============================================================
#  CONFIG CENTRALE — Smart Library Lakehouse (Apache Iceberg)
#  Importé dans tous les notebooks : from config import *
# ============================================================

# ── Connexion MinIO (stockage objet des données Iceberg) ──────
MINIO_ENDPOINT  = "http://minio:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin123"
BUCKET          = "s3a://lakehouse"

# ── Connexion Lakekeeper (catalogue REST Iceberg) ─────────────
LAKEKEEPER_URI       = "http://lakekeeper:8181/catalog"
LAKEKEEPER_WAREHOUSE = "lakehouse"
CATALOG_NAME         = "lakehouse"   # nom du catalogue Spark (spark.sql.catalog.lakehouse)

# ── Namespaces (équivalents des dossiers Medallion) ───────────
NS_BRONZE = f"{CATALOG_NAME}.bronze"
NS_SILVER = f"{CATALOG_NAME}.silver"
NS_GOLD   = f"{CATALOG_NAME}.gold"

# ── Tables Bronze (catalogue.namespace.table) ─────────────────
BRONZE_UTILISATEURS = f"{NS_BRONZE}.utilisateurs"
BRONZE_LIVRES       = f"{NS_BRONZE}.livres"
BRONZE_COMMANDES    = f"{NS_BRONZE}.commandes"
BRONZE_STOCK_COW    = f"{NS_BRONZE}.stock_cow"
BRONZE_STOCK_MOR    = f"{NS_BRONZE}.stock_mor"
BRONZE_CATALOGUE    = f"{NS_BRONZE}.catalogue"
BRONZE_FACTURES     = f"{NS_BRONZE}.factures"
BRONZE_CLICS        = f"{NS_BRONZE}.evenements_clics"
BRONZE_RECHERCHES   = f"{NS_BRONZE}.evenements_recherches"
BRONZE_PANIER       = f"{NS_BRONZE}.evenements_panier"
BRONZE_AVIS         = f"{NS_BRONZE}.evenements_avis"

# ── Tables Silver ─────────────────────────────────────────────
SILVER_UTILISATEURS = f"{NS_SILVER}.utilisateurs"
SILVER_LIVRES       = f"{NS_SILVER}.livres"
SILVER_COMMANDES    = f"{NS_SILVER}.commandes"
SILVER_STOCK        = f"{NS_SILVER}.stock"
SILVER_CATALOGUE    = f"{NS_SILVER}.catalogue"
SILVER_ACTIVITE     = f"{NS_SILVER}.activite_temps_reel"

# ── Tables Gold ───────────────────────────────────────────────
GOLD_TOP_LIVRES        = f"{NS_GOLD}.top_livres"
GOLD_VENTES_PERIODE    = f"{NS_GOLD}.ventes_periode"
GOLD_COMPORTEMENT      = f"{NS_GOLD}.comportement_utilisateurs"
GOLD_HISTORIQUE_VENTES = f"{NS_GOLD}.historique_ventes"
GOLD_TENDANCES         = f"{NS_GOLD}.tendances"
GOLD_TENDANCES_RT      = f"{NS_GOLD}.tendances_temps_reel"
GOLD_RECOMMANDATIONS   = f"{NS_GOLD}.recommandations"

# ── Kafka (inchangé) ──────────────────────────────────────────
KAFKA_BOOTSTRAP      = "kafka:29092"
TOPIC_CLICS          = "clics-utilisateurs"
TOPIC_RECHERCHES     = "recherches-utilisateurs"
TOPIC_PANIER         = "evenements-panier"
TOPIC_AVIS           = "avis-livres"

# ── Paramètres de table Iceberg (équivalent de hudi_options) ──
#
#   Avec Hudi, le "type de table" (COW/MOR) et la clé d'upsert se
#   déclarent au niveau de l'ÉCRITURE (options du DataFrameWriter).
#   Avec Iceberg, ce sont des PROPRIÉTÉS DE TABLE (déclarées une fois
#   à la création) : write.delete.mode / write.update.mode /
#   write.merge.mode = "copy-on-write" ou "merge-on-read".
#
def iceberg_table_properties(type_table="copy-on-write", format_version=2):
    """
    Retourne les propriétés de table Iceberg équivalentes au hudi_options.

    type_table :
      - "copy-on-write" → batch, lectures fréquentes (COW, comme Hudi COW)
      - "merge-on-read"  → écritures fréquentes / streaming (MOR, comme Hudi MOR)
    """
    return {
        "format-version"      : str(format_version),
        "write.delete.mode"   : type_table,
        "write.update.mode"   : type_table,
        "write.merge.mode"    : type_table,
        # équivalent de l'auto-compaction Hudi pour MOR
        "commit.manifest.min-count-to-merge": "10",
    }


def create_or_replace_table(df, table_name, partition_cols=None,
                             type_table="copy-on-write"):
    """
    Crée (ou remplace intégralement) une table Iceberg à partir d'un DataFrame.
    Équivalent de : df.write.format("hudi").options(**hudi_options(...)).mode("overwrite").save(chemin)
    """
    # Iceberg (ClusteredWriter) exige que les lignes soient triées/regroupées
    # par colonne de partition AVANT l'écriture — contrairement à Hudi qui
    # gère le clustering en interne.
    if partition_cols:
        df = df.sort(*partition_cols)
    writer = df.writeTo(table_name)
    if partition_cols:
        from pyspark.sql import functions as F
        writer = writer.partitionedBy(*[F.col(c) for c in partition_cols])
    for k, v in iceberg_table_properties(type_table).items():
        writer = writer.tableProperty(k, v)
    writer.createOrReplace()


def append_table(df, table_name):
    """Ajoute des lignes à une table Iceberg existante (insert simple, pas d'upsert)."""
    df.writeTo(table_name).append()


def upsert_table(spark, df, table_name, key_cols, updated_at_col="updated_at"):
    """
    UPSERT Iceberg via MERGE INTO — équivalent fonctionnel de l'UPSERT
    automatique de Hudi (déclenché par hoodie.datasource.write.recordkey.field).

    Contrairement à Hudi, Iceberg n'a pas de notion de "clé d'enregistrement"
    intrinsèque à la table : l'UPSERT doit être exprimé explicitement en SQL.
    """
    vue_staging = f"_staging_{table_name.split('.')[-1]}"
    df.createOrReplaceTempView(vue_staging)
    condition = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    spark.sql(f"""
        MERGE INTO {table_name} t
        USING {vue_staging} s
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def spark_iceberg_conf(spark_builder):
    """
    Configure une SparkSession avec Iceberg + catalogue REST Lakekeeper + MinIO.
    Usage :
        spark = spark_iceberg_conf(SparkSession.builder.appName("...")).getOrCreate()
    """
    packages = ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
    ])
    return (
        spark_builder
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", LAKEKEEPER_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", LAKEKEEPER_WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", MINIO_ACCESS)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", MINIO_SECRET)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        # fs.s3a nécessaire pour les lectures directes hors-Iceberg (CSV/JSON/XML sources)
        .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
    )


def creer_namespaces(spark):
    """Crée les 3 namespaces Medallion (bronze/silver/gold) s'ils n'existent pas."""
    for ns in (NS_BRONZE, NS_SILVER, NS_GOLD):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


# ── DGLH : jeu de données « grande surface tunisienne » (voir generateur/) ──────────────
# Le générateur écrit dans data/raw (full) ou data/raw_echantillon (sample).
# Dans le conteneur Jupyter : /home/jovyan/data/raw  — à copier vers MinIO (zone raw) pour la phase bronze.
RAW_DIR = "/home/jovyan/data/raw"
RAW_ECHANTILLON_DIR = "/home/jovyan/data/raw_echantillon"
MAGASINS = [f"magasin_{n:02d}" for n in range(1, 21)]      # partition bronze envisagée : magasin_id, tri par date

# ── Mode distribué ────────────────────────────────────────────
# MinIO distribué (4 nœuds) derrière nginx : même URL qu'avant -> http://minio:9000
BUCKET_RAW = "s3a://raw"                      # génération complète
BUCKET_RAW_ECHANTILLON = "s3a://raw-echantillon"
SPARK_MASTER = "spark://spark-master:7077"
