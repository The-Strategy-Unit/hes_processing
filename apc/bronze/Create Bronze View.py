# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW hes.bronze.apc
# MAGIC COMMENT '# Admitted Patient Care (APC)\n\nThis table contains the admitted patient care dataset from HES. It is episode level, so each admission will contain 1 or more rows in this data.\n\nFor further information, see [HES Data Dictionary](https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics/hospital-episode-statistics-data-dictionary)'
# MAGIC
# MAGIC AS
# MAGIC
# MAGIC SELECT * FROM PARQUET.`/Volumes/hes/bronze/raw/apc`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fyear, COUNT(*) n
# MAGIC FROM   hes.bronze.apc
# MAGIC GROUP BY fyear
# MAGIC ORDER BY fyear;
