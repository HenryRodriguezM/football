# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Widgets

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_football")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexion a fuentes

# COMMAND ----------

# Recuperar credenciales de acceso de Azure Key Vault
scope = "accessScopeforADLS"
user = dbutils.secrets.get(scope="accessScopeforADLS", key="sqldb-user")
password = dbutils.secrets.get(scope="accessScopeforADLS", key="sqldb-pass")

# Definir variables de conexión para Azure SQL Server
jdbc_hostname = "svrprddbfootball.database.windows.net"
jdbc_port = 1433
jdbc_database = "FootballGamesDB"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de fuentes

# COMMAND ----------

df_teams = spark.table(f"{catalogo}.{esquema_source}.tbl_joined_final")

# COMMAND ----------

df_teams.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_teams")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de tabla: Rendimiento de clubes por temporada

# COMMAND ----------

df_club_performance = spark.table(f"{catalogo}.{esquema_source}.tbl_club_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de tabla: Valor de mercado y perfil de jugadores

# COMMAND ----------

df_player_market = spark.table(f"{catalogo}.{esquema_source}.tbl_player_market")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de tabla: Resultados y asistencia por partido

# COMMAND ----------

df_match_analysis = spark.table(f"{catalogo}.{esquema_source}.tbl_match_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardar Tablas

# COMMAND ----------

# escribir en la tabla en Azure SQL Database
df_golden_teams = spark.table(f"{catalogo}.{esquema_sink}.golden_teams")

(
    df_golden_teams.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.tbl_golden_teams")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .mode("overwrite")
    .save()
)

(
    df_club_performance.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.tbl_club_performance")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .mode("overwrite")
    .save()
)

(
    df_player_market.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.tbl_player_market")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .mode("overwrite")
    .save()
)

(
    df_match_analysis.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.tbl_match_analysis")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .mode("overwrite")
    .save()
)
