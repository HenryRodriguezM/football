# Databricks notebook source
# MAGIC %md
# MAGIC ### Crear grupo 'Data_Engineers' y agregar usuarios

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP `Data_Engineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER GROUP `Data_Engineers`
# MAGIC ADD USER `rodriguez.montero.henry@outlook.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER GROUP `Data_Engineers`
# MAGIC ADD USER `henryrm33@gmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Otorgar permisos al catalogo y los esquemas

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG catalog_football TO `Data_Engineers`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA catalog_football.bronze TO `Data_Engineers`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA catalog_football.silver TO `Data_Engineers`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA catalog_football.golden TO `Data_Engineers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Otorgar permisos a External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-raw` TO `Data_Engineers`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-bronze` TO `Data_Engineers`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-silver` TO `Data_Engineers`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-golden` TO `Data_Engineers`;
