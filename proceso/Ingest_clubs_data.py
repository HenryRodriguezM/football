# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Widgets

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_football")
dbutils.widgets.text("esquema", "bronze")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion a fuentes

# COMMAND ----------

# NUEVA FUENTE: BLOB STORAGE
storage_account = "ablsmartdata2026"
scope = "accessScopeforADLS"
key = "BlobStorageToken" # nuevo Secreto en Azure Key Vault: Secret value = SAS Token a nivel de Container

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=f"{scope}", key=f"{key}"))

# COMMAND ----------

# NUEVA FUENTE: BLOB STORAGE
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@ablsmartdata2026.dfs.core.windows.net/clubs.csv"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Schema

# COMMAND ----------

clubs_schema = StructType(fields=[
    StructField("club_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("pretty_name", StringType(), True),
    StructField("domestic_competition_id", StringType(), True),
    StructField("total_market_value", DoubleType(), True),
    StructField("squad_size", IntegerType(), True),
    StructField("average_age", DoubleType(), True),
    StructField("foreigners_number", IntegerType(), True),
    StructField("foreigners_percentage", DoubleType(), True),
    StructField("national_team_players", IntegerType(), True),
    StructField("stadium_name", StringType(), True),
    StructField("stadium_seats", IntegerType(), True),
    StructField("net_transfer_record", StringType(), True),
    StructField("coach_name", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de fuentes

# COMMAND ----------

df_clubs = spark.read \
            .option("header", True) \
            .schema(clubs_schema) \
            .csv(ruta)

# COMMAND ----------

df_clubs_selected = df_clubs.select(col("club_id"),
                                    col("pretty_name"),
                                    col("foreigners_number"),
                                    col("foreigners_percentage"),
                                    col("national_team_players")
                                    )

# COMMAND ----------

df_clubs_renamed = df_clubs_selected.withColumnRenamed("pretty_name", "club_name") \
                                        .withColumnRenamed("foreigners_number", "foreigners")

# COMMAND ----------

df_clubs_final = df_clubs_renamed.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar Tabla Clubs

# COMMAND ----------

df_clubs_final.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.clubs')
