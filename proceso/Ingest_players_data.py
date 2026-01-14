# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Widgets

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_football")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@adlsmartdata2026.dfs.core.windows.net/players.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Schema

# COMMAND ----------

players_schema = StructType(fields=[
    StructField("player_id", IntegerType(), False),
    StructField("current_club_id", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("pretty_name", StringType(), True),
    StructField("country_of_birth", StringType(), True),
    StructField("country_of_citizenship", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("position", StringType(), True),
    StructField("sub_position", StringType(), True),
    StructField("foot", StringType(), True),
    StructField("height_in_cm", IntegerType(), True),
    StructField("market_value_in_gbp", DoubleType(), True),
    StructField("highest_market_value_in_gbp", DoubleType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de fuentes

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_players = spark.read.option('header', True).schema(players_schema).csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
df_players_selected = df_players.select(col("player_id"), 
                                                col("current_club_id"),
                                                col("pretty_name"), 
                                                col("country_of_birth"), 
                                                col("country_of_citizenship"), 
                                                col("date_of_birth"), 
                                                col("position"), 
                                                col("sub_position"), 
                                                col("foot"), 
                                                col("height_in_cm"), 
                                                col("market_value_in_gbp"), 
                                                col("highest_market_value_in_gbp"))

# COMMAND ----------

df_players_renamed = df_players_selected.withColumnRenamed("pretty_name", "full_name") \
                                            .withColumnRenamed("country_of_birth", "country") \
                                            .withColumnRenamed("country_of_citizenship", "nationality") \
                                            .withColumnRenamed("date_of_birth", "birth_date") \
                                            .withColumnRenamed("height_in_cm", "height") \
                                            .withColumnRenamed("market_value_in_gbp", "market_value") \
                                            .withColumnRenamed("highest_market_value_in_gbp", "highest_market_value")

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
df_players_final = df_players_renamed.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar Tabla Players

# COMMAND ----------

df_players_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.players")
