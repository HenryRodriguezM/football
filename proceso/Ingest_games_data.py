# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
from pyspark.sql.functions import col

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


# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion a fuentes

# COMMAND ----------

# NUEVA FUENTE: Azure SQL Database

# Recuperar credenciales de acceso de Azure Key Vault
scope = "accessScopeforADLS"
user = dbutils.secrets.get(scope="accessScopeforADLS", key="sqldb-user")
password = dbutils.secrets.get(scope="accessScopeforADLS", key="sqldb-pass")

# Definir variables de conexión para Azure SQL Server
jdbc_hostname = "svrprddbfootball.database.windows.net"
jdbc_port = 1433
jdbc_database = "FootballGamesDB"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

# Leer la tabla desde Azure SQL Database
df_games = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", "staging.games") \
  .option("user", user) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Schema

# COMMAND ----------

games_schema = StructType(fields=[
    StructField("game_id", IntegerType(), False),
    StructField("competition_id", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("rond", StringType(), True),
    StructField("game_date", DateType(), True),
    StructField("home_club_id", IntegerType(), True),
    StructField("away_club_id", IntegerType(), True),
    StructField("home_club_goals", IntegerType(), True),
    StructField("away_club_goals", IntegerType(), True),
    StructField("home_club_position", IntegerType(), True),
    StructField("away_club_position", IntegerType(), True),
    StructField("stadium", StringType(), True),
    StructField("attendance", IntegerType(), True),
    StructField("referee", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df_games_selected = df_games.select(col("game_id"),
                                    col("season"),
                                    col("rond"),
                                    col("game_date"),
                                    col("home_club_id"),
                                    col("away_club_id"),
                                    col("home_club_goals"),
                                    col("away_club_goals"),
                                    col("home_club_position"),
                                    col("away_club_position"),
                                    col("stadium"),
                                    col("attendance"),
                                    col("referee")
)

# COMMAND ----------

df_games_final = df_games_selected.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar Tabla Games

# COMMAND ----------

df_games_final.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.games')
