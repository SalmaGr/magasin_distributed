"""
ingestion_bronze.py — Ingestion DISTRIBUÉE raw (MinIO/S3, s3a://) -> bronze (tables Apache Iceberg via Lakekeeper).

    spark-submit /home/jovyan/spark_jobs/ingestion_bronze.py --bucket raw-echantillon
    spark-submit ... --bucket raw --sources ventes_tickets,ventes_lignes --compaction
Le calcul est réparti sur les workers du cluster (lecture des fichiers, écriture des fichiers Parquet).

Bronze = données brutes telles que livrées (tout en chaîne) + colonnes techniques :
  _fichier, _ingere_le, _mois (lot), magasin_id (si applicable), dt_fichier (date lue dans le nom du fichier).
  • CSV  : un lot par mois ; les colonnes ajoutées (évolution de schéma) sont ajoutées à la table par ALTER TABLE ADD COLUMN.
           Le renommage (mp -> mode_paiement, unité -> cond_vte) donne deux colonnes : la réconciliation relève de la couche silver.
  • JSONL: conservés bruts dans la colonne ligne_json (schéma stable ; le parsing relève de silver).
  • JSON (un document par fichier, catalogue e-commerce) : contenu brut dans json_brut, une ligne par fichier.
  • XML (factures fournisseurs) : contenu brut dans xml_brut, une ligne par fichier (le parsing relève de silver).
  • PDF (contrats, fiches techniques) : table de MÉTADONNÉES uniquement (chemin, type, fournisseur, produit, version, taille,
           date de modification, empreinte SHA-256). Les fichiers restent dans le bucket raw ; le texte sera extrait en silver.
  • Partition : magasin_id (20 partitions) ; tri par dt_fichier ; fichiers cibles ≈ 128 Mo.
  • Reprenable : les lots (_mois) déjà présents sont ignorés ; --reset supprime et recrée les tables.
Limite : un lot (mois) lit tous ses fichiers avec l'en-tête du premier ; les évolutions de schéma doivent donc tomber sur
une frontière de mois (c'est le cas du config.yaml fourni).
"""
import argparse
import re
import time

from pyspark.sql import SparkSession, functions as F

CAT = "lakehouse"
NS = f"{CAT}.bronze"
TABLES_PRODUITS = ["prod_a", "art_02", "t_prd_03", "ref_produit_04", "catalogue_05", "items_06", "articles_07", "prod_x1", "art_divers"]

# nom -> (format, glob des fichiers relatif au bucket, partitionné par magasin ?)
SPECS = {
    "ventes_tickets": ("csv", "ventes/tickets/*/annee=*/mois=*/*.csv*", True),
    "ventes_lignes": ("csv", "ventes/lignes/*/annee=*/mois=*/*.csv*", True),
    "ventes_paiements": ("csv", "ventes/paiements/*/annee=*/mois=*/*.csv*", True),
    "stock_mouvements": ("csv", "erp/stock/*/annee=*/mois=*/*.csv*", True),
    "pos_logs": ("json", "pos_logs/*/annee=*/mois=*/*.jsonl*", True),
    "clients_crm": ("csv", "clients/crm/*.csv*", False),
    "clients_fidelite": ("csv", "clients/fidelite/*.csv*", False),
    "clients_web": ("json", "clients/ecommerce/*.jsonl*", False),
    "ecommerce_commandes": ("json", "ecommerce/commandes/annee=*/mois=*/*.jsonl*", False),
    "ecommerce_clickstream": ("json", "ecommerce/clickstream/annee=*/mois=*/*.jsonl*", False),
    "hist_prix": ("csv", "erp/hist_prix/*.csv*", False),
    "promos": ("csv", "erp/promos/*.csv*", False),
    "fournisseurs_fiches": ("csv", "fournisseurs/fiches/*.csv*", False),
    "fournisseurs_tarifs": ("csv", "fournisseurs/tarifs/*.csv*", False),
    "rh_employes": ("csv", "rh/employes/*.csv*", False),
    "referentiel_magasins": ("csv", "referentiel/magasins/*.csv*", False),
    "avis_formulaire": ("csv", "avis/formulaire_site/*.csv*", False),
    "avis_reseaux_sociaux": ("json", "avis/reseaux_sociaux/annee=*/mois=*/*.jsonl*", False),
    "avis_emails": ("json", "avis/emails/*.jsonl*", False),
    "appels_transcriptions": ("json", "appels/transcriptions/*.jsonl*", False),
}
# Autres types : le « glob » peut être une liste (plusieurs dossiers vers une même table)
SPECS["ecommerce_catalogue_api"] = ("json_doc", "ecommerce/catalogue_api/annee=*/mois=*/*.json*", False)
SPECS["fournisseurs_factures_xml"] = ("xml", "fournisseurs/factures_xml/magasin_*/annee=*/mois=*/*.xml*", True)
SPECS["documents_pdf"] = ("pdf", ["fournisseurs/contrats_pdf/*.pdf", "fournisseurs/fiches_techniques_pdf/*/*.pdf"], False)
for t in TABLES_PRODUITS:                                   # noms de tables opaques conservés tels quels (utile à l'enrichissement métier)
    SPECS[t] = ("csv", f"erp/produits/{t}/*.csv*", False)

RE_MOIS = re.compile(r"annee=(\d{4})/mois=(\d{2})|(\d{4})-(\d{2})(?:-\d{2})?")


def cle_mois(chemin):
    m = RE_MOIS.search(chemin)
    if not m:
        return "inconnu"
    return f"{m.group(1) or m.group(3)}-{m.group(2) or m.group(4)}"


def lister(spark, motif):
    """Liste distribuée (API Hadoop FS) des fichiers correspondant au glob s3a://..."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(motif)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    st = fs.globStatus(p)
    return sorted(s.getPath().toString() for s in (st or []) if s.isFile())


def table_existe(spark, nom):
    return spark.catalog.tableExists(nom)


def lots_deja_charges(spark, nom):
    if not table_existe(spark, nom):
        return set()
    return {r[0] for r in spark.table(nom).select("_mois").distinct().collect()}


def vide_en_null(col):
    return F.when(col == "", None).otherwise(col)


def lire_lot(spark, fmt, fichiers, par_magasin):
    if fmt == "pdf":
        # métadonnées seulement : le contenu est lu pour l'empreinte puis abandonné (calcul réparti sur les workers)
        df = (spark.read.format("binaryFile").load(fichiers)
              .select(F.col("path").alias("_fichier"),
                      F.col("length").cast("string").alias("taille_octets"),
                      F.col("modificationTime").cast("string").alias("modifie_le"),
                      F.sha2(F.col("content"), 256).alias("sha256")))
    else:
        if fmt == "csv":
            df = spark.read.option("header", True).option("inferSchema", False).option("multiLine", False).csv(fichiers)
        elif fmt == "json":
            df = spark.read.text(fichiers).withColumnRenamed("value", "ligne_json")
        else:   # "xml" ou "json_doc" : un fichier = une ligne (texte entier)
            colonne = "xml_brut" if fmt == "xml" else "json_brut"
            df = spark.read.option("wholetext", True).text(fichiers).withColumnRenamed("value", colonne)
        df = df.withColumn("_fichier", F.input_file_name())
    if fmt in ("xml", "json_doc", "pdf"):
        df = df.withColumn("nom_fichier", F.regexp_extract("_fichier", r"([^/]+)$", 1))
    if fmt == "pdf":
        est_contrat = F.col("_fichier").contains("/contrats_pdf/")
        id_contrat = vide_en_null(F.regexp_extract("nom_fichier", r"^contrat_(.+)_v\d+\.pdf$", 1))
        dossier_frn = vide_en_null(F.regexp_extract("_fichier", r"fiches_techniques_pdf/([^/]+)/", 1))
        df = (df.withColumn("type_document", F.when(est_contrat, "contrat").otherwise("fiche_technique"))
                .withColumn("id_fournisseur", F.coalesce(id_contrat, dossier_frn))
                .withColumn("code_produit", vide_en_null(F.regexp_extract("nom_fichier", r"^fiche_(.+)_v\d+\.pdf$", 1)))
                .withColumn("version", vide_en_null(F.regexp_extract("nom_fichier", r"_v(\d+)\.pdf$", 1))))
    df = df.withColumn("dt_fichier", F.regexp_extract("_fichier", r"(\d{4}-\d{2}-\d{2})", 1))
    df = df.withColumn("dt_fichier", F.when(F.col("dt_fichier") == "", None).otherwise(F.col("dt_fichier")))
    if par_magasin:
        df = df.withColumn("magasin_id", F.regexp_extract("_fichier", r"(magasin_\d{2})", 1))
    return df.withColumn("_ingere_le", F.current_timestamp())


def aligner_table(spark, nom, df, par_magasin):
    """Crée la table au premier lot ; ensuite ajoute les colonnes nouvelles (évolution de schéma Iceberg)."""
    if not table_existe(spark, nom):
        w = df.limit(0).writeTo(nom).tableProperty("write.target-file-size-bytes", "134217728") \
            .tableProperty("write.parquet.compression-codec", "zstd")
        if par_magasin:
            w = w.partitionedBy(F.col("magasin_id"))
        w.create()
        return
    existantes = [c.name for c in spark.table(nom).schema.fields]
    for c in df.columns:
        if c not in existantes:
            spark.sql(f"ALTER TABLE {nom} ADD COLUMN `{c}` string")
            print(f"    schéma : colonne ajoutée {c}", flush=True)


def ingerer(spark, nom, bucket, reset):
    fmt, motif, par_magasin = SPECS[nom]
    table = f"{NS}.{nom}"
    if reset:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    motifs = motif if isinstance(motif, list) else [motif]
    fichiers = sorted({f for m in motifs for f in lister(spark, f"s3a://{bucket}/{m}")})
    if not fichiers:
        print(f"  {nom}: aucun fichier ({', '.join(motifs)})", flush=True)
        return 0
    lots = {}
    for f in fichiers:
        lots.setdefault(cle_mois(f), []).append(f)
    deja = lots_deja_charges(spark, table)
    total = 0
    for mois in sorted(lots):
        if mois in deja:
            continue
        t0 = time.time()
        df = lire_lot(spark, fmt, lots[mois], par_magasin).withColumn("_mois", F.lit(mois))
        aligner_table(spark, table, df, par_magasin)
        cols = [c.name for c in spark.table(table).schema.fields]
        for c in cols:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))
        df = df.select(*cols)
        if par_magasin:
            df = df.repartition(F.col("magasin_id")).sortWithinPartitions("magasin_id", "dt_fichier")
        df.writeTo(table).append()
        total += 1
        print(f"  {nom} {mois}: {len(lots[mois])} fichiers ingérés ({time.time() - t0:.0f}s)", flush=True)
    return total


def compacter(spark, nom):
    t = f"{NS}.{nom}"
    if table_existe(spark, t):
        spark.sql(f"CALL {CAT}.system.rewrite_data_files(table => 'bronze.{nom}', options => map('target-file-size-bytes','134217728'))")
        spark.sql(f"CALL {CAT}.system.expire_snapshots(table => 'bronze.{nom}', retain_last => 3)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default="raw-echantillon")
    ap.add_argument("--sources", help="liste séparée par des virgules (défaut : toutes)")
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--compaction", action="store_true")
    a = ap.parse_args()
    spark = SparkSession.builder.appName("dglh-ingestion-bronze").getOrCreate()   # master et catalogue : spark-defaults.conf
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    noms = a.sources.split(",") if a.sources else list(SPECS)
    print(f"Cluster : {spark.sparkContext.master} · parallélisme {spark.sparkContext.defaultParallelism} · bucket s3a://{a.bucket}", flush=True)
    for n in noms:
        ingerer(spark, n, a.bucket, a.reset)
        if a.compaction:
            compacter(spark, n)
    print("\nRésumé bronze :")
    for n in noms:
        t = f"{NS}.{n}"
        if table_existe(spark, t):
            print(f"  {t}: {spark.table(t).count():,} lignes")
    spark.stop()


if __name__ == "__main__":
    main()