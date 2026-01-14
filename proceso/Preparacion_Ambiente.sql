-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creación de Widgets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storageName default "adlsmartdata2026";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de Catalog

-- COMMAND ----------

DROP CATALOG IF EXISTS catalog_football CASCADE;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_football;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_football.raw;
CREATE SCHEMA IF NOT EXISTS catalog_football.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_football.silver;
CREATE SCHEMA IF NOT EXISTS catalog_football.golden;
CREATE SCHEMA IF NOT EXISTS catalog_football.exploratory;


CREATE VOLUME IF NOT EXISTS catalog_football.raw.datasets;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de External Locations

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas raw del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas silver del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas golden del Data Lake';
