"""Bronze en local avec PySpark : landing/erp (JSON par page) -> lakehouse/bronze (Parquet).
 
Même transformation que le notebook Databricks 02_bronze_autoloader, en mode batch :
une ligne par enregistrement, JSON brut conservé, colonnes de traçabilité, doublons compris.
Le nettoyage (dédoublonnage, dates, devises…) est le travail du Silver.
 
Usage, depuis la racine du dépôt :
    python scripts/bronze_local.py                    # les trois entités
    python scripts/bronze_local.py suppliers          # une seule
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
LANDING = "landing/erp"
BRONZE = "lakehouse/bronze"
TABLES = {
    "suppliers": "erp_suppliers",
    "parts": "erp_parts",
    "purchase-orders": "erp_purchase_orders",
}
# Schéma volontairement minimal : chaque élément de data reste un JSON brut (string), les infos vient de json.dump(data) dans simple_ingest.py
PAGE_SCHEMA = "data ARRAY<STRING>, _run_id STRING, _fetched_at STRING"
spark =(SparkSession.builder
        .appName("agrimeca-bronze-local")
        .master("local[*]") #tous les coeurs du Mac
        .config("spark.driver.memory", "4g")  # 4 Go de RAM pour le driver (le Mac a 16 Go)
        .config("spark.sql.session.timeZone", "UTC")  # pour que les dates soient cohérentes avec l'API ERP
        .getOrCreate())
spark.sparkContext.setLogLevel("WARN")  # INFO est trop bavard, DEBUG est inutile : moins de messages, mais les erreurs sont toujours visibles

def bronze_entity(entity: str, table_name: str):
 # 1. Lecture : un fichier = une page = une ligne de texte (équivalent de cloudFiles.format=text + wholetext)
    pages = (spark.read
          .option("wholetext", True)  # un fichier = une ligne de texte"
          .option("recursiveFileLookup", True)  # lire tous les fichiers du dossier et sous-dossiers
          .option("pathGlobFilter", "page_*.json")  # uniquement les pages JSON, pas les fichiers _SUCCESS ou _committed
          .text(f"{LANDING}/{entity}")
          .withColumn("_file",F.col("_metadata.file_path"))  # Pourquoi c’est utile : la traçabilité. Chaque ligne du Bronze sait de quel fichier elle vient, donc de quelle page, donc de quel run_id, donc de quelle date d’ingestion.
            )

 # 2. Une ligne par enregistrement, JSON brut conservé + colonnes techniques
    records = (pages
               .select(F.from_json("value",PAGE_SCHEMA).alias("p"),"_file")  # Cette ligne transforme le texte de chaque page en données structurées, tout en gardant la colonne _file pour la traçabilité.
               .select("p._run_id","p._fetched_at","_file", F.explode("p.data").alias("payload"))  # On ne garde que les colonnes utiles : run_id, fetched_at, data (liste de JSON), et _file pour la traçabilité.
               .select(F.get_json_object("payload", "$.id").alias("source_id"),
                       F.get_json_object("payload", "$.audit.updated_at").alias("source_updated_at"),
                       "payload", "_run_id", "_fetched_at", "_file",
                       F.current_timestamp().alias("_ingest_ts"))
                )
# 3. Écriture : reconstruction complète, partitionnée par run (relancer donne le même résultat)
    path = f"{BRONZE}/{table_name}"
    records.write.mode("overwrite").partitionBy("_run_id").parquet(path)
    
    # 4. Contrôle
    df = spark.read.parquet(path)
    print(f"{table_name} : {df.count()} lignes, {df.select('source_id').distinct().count()} ids distincts")
    df.groupBy("_run_id").count().orderBy("_run_id").show(truncate=False)

# appel de la fonction pour chaque entité passée en argument recupéré du terminal, ou toutes les entités du dictionnaire TABLES
entities = sys.argv[1:] or list(TABLES) 
for entity in entities:
    bronze_entity(entity, TABLES[entity])
 
spark.stop()
