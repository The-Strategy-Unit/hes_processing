# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Check counts by provider
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from databricks.sdk.runtime import *

# COMMAND ----------

df = (
    spark.read.table("hes.silver.aae")
    .groupBy(["fyear", "procode3"])
    .count()
    .sort(["fyear", "procode3"])
).display()
