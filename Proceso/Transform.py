# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Widgets

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_football")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de UDF

# COMMAND ----------

def calculo_edad(fecha_nacimiento):
    fecha_actual = datetime.now()
    edad = fecha_actual.year - fecha_nacimiento.year
    return edad

# COMMAND ----------

def categoria_edad(edad):
    if edad < 25:
        return "Joven"
    elif 25 <= edad < 30:
        return "Mayor"
    else:
        return "Veterano"

# COMMAND ----------

def categoria_altitud(altitud):
    if altitud < 170:
        return "Bajo"
    elif 170 <= altitud < 180:
        return "Medio"
    else:
        return "Alto"

# COMMAND ----------

def resultado_juegos(goles_local, goles_visitante):
    if goles_local > goles_visitante:
        return "Ganador_local"
    elif goles_local < goles_visitante:
        return "Ganador_visitante"
    else:
        return "Empate"

# COMMAND ----------

calculo_edad_udf = F.udf(calculo_edad, IntegerType())
categoria_edad_udf = F.udf(categoria_edad, StringType())
categoria_altitud_udf = F.udf(categoria_altitud, StringType())

resultado_udf = F.udf(resultado_juegos, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de fuentes

# COMMAND ----------

df_players = spark.table(f"{catalogo}.{esquema_source}.players")
df_clubs = spark.table(f"{catalogo}.{esquema_source}.clubs")
df_games = spark.table(f"{catalogo}.{esquema_source}.games")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpieza y transformación

# COMMAND ----------

df_players = df_players.dropna(how="all")\
                .filter((col("player_id").isNotNull()) | (col("current_club_id")).isNotNull())

df_clubs = df_clubs.dropna(how="all")\
                .filter((col("club_id").isNotNull()) | (col("club_name")).isNotNull())

df_games = df_games.dropna(how="all")\
                .filter((col("game_id").isNotNull()) | (col("home_club_id")).isNotNull() | (col("away_club_id")).isNotNull())


# COMMAND ----------

df_players = df_players.filter(col("market_value") > 0)
df_games = df_games.filter((col("attendance") > 0))

# COMMAND ----------

df_players = df_players.withColumn("age", calculo_edad_udf("birth_date"))
df_players = df_players.withColumn("age_category", categoria_edad_udf("age"))
df_players = df_players.withColumn("height_category", categoria_altitud_udf("height"))

df_games = df_games.withColumn("match_result", resultado_udf("home_club_goals", "away_club_goals"))

# COMMAND ----------

df_players = df_players.drop(col("ingestion_date"))
df_clubs = df_clubs.drop(col("ingestion_date"))

# COMMAND ----------

df_clubs_players = df_clubs.alias("x").join(df_players.alias("y"), col("x.club_id") == col("y.current_club_id"), "inner")

df_joined_final = df_clubs_players.alias("cp").join(df_games.alias("z"), col("cp.club_id") == col("z.home_club_id"), "left")

# COMMAND ----------

df_joined_final.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_joined_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Rendimiento de clubes por temporada

# COMMAND ----------

club_performance = df_joined_final.groupBy(
    "season", "club_id", "club_name"
).agg(
    count("game_id").alias("games_played"),
    sum("home_club_goals").alias("goals_scored"),
    avg("attendance").alias("avg_attendance")
)

club_performance.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_club_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Valor de mercado y perfil de jugadores

# COMMAND ----------

player_market = df_joined_final.groupBy(
    "season", "player_id", "full_name", "club_name", "position"
).agg(
    avg("market_value").alias("avg_market_value"),
    avg("age").alias("avg_age"),
    avg("height").alias("avg_height")
)

player_market.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_player_market"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla: Resultados y asistencia por partido

# COMMAND ----------

match_analysis = df_joined_final.select(
    "season", "game_id", "game_date",
    "club_name", "stadium", "attendance",
    "home_club_goals", "away_club_goals",
    "match_result"
)

match_analysis.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_match_analysis"
)
