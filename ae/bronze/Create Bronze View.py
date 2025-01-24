# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW hes.bronze.aae
# MAGIC COMMENT '# Accident and Emergency (AaE)\n\nThis table contains the A&E dataset from HES.\n\nFor further information, see [HES Data Dictionary](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)'
# MAGIC
# MAGIC AS
# MAGIC
# MAGIC SELECT * FROM PARQUET.`/Volumes/hes/bronze/raw/ae`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fyear, COUNT(*) n
# MAGIC FROM   hes.bronze.aae
# MAGIC GROUP BY fyear
# MAGIC ORDER BY fyear;
