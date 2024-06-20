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

admimeth = F.col("admimeth")

df = (
    spark.read.table("hes.silver.apc")
    .filter(F.col("last_episode_in_spell") == True)
    .withColumn(
        "admission_group",
        F
            .when(admimeth.rlike("^2"), "emergency")
            .when(admimeth.rlike("^1"), "elective")
            .otherwise("other")
    )
    .groupBy(["fyear", "procode3"])
    .pivot("admission_group")
    .count()
    .sort(["fyear", "procode3"])
    .na.fill(0)
)

# COMMAND ----------

display(df)
