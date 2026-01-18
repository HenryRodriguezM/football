# Databricks notebook source
# MAGIC %md
# MAGIC ### Creación de Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default "adlsmartdata2026";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS catalog_football CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_football;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_football.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_football.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_football.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_football.golden;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_football.exploratory;
# MAGIC
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS catalog_football.raw.datasets;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
# MAGIC URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas raw del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
# MAGIC URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
# MAGIC URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas silver del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
# MAGIC URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas golden del Data Lake';
