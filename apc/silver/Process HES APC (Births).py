# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

from databricks.sdk.runtime import *

# COMMAND ----------

df = spark.read.table("hes.bronze.apc")

# COMMAND ----------

dfs = [
    df.select(
        "epikey",
        "fyear",
        "procode3",
        F.col(f"biresus_{i}").alias("biresus"),
        F.col(f"birordr_{i}").alias("birordr"),
        F.col(f"birstat_{i}").alias("birstat"),
        F.col(f"birweit_{i}").alias("birweit"),
        F.col(f"delmeth_{i}").alias("delmeth"),
        F.col(f"delplac_{i}").alias("delplac"),
        F.col(f"delstat_{i}").alias("delstat"),
        F.col(f"gestat_{i}").alias("gestat"),
        F.col(f"sexbaby_{i}").alias("sexbaby")
    ).filter(f"!biresus is null or !birordr is null or !birstat is null or !birweit is null or !delmeth is null or !delplac is null or !delstat is null or !gestat is null or !sexbaby is null")
    for i in range(1, 10)
]

birth_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

# COMMAND ----------


(
    birth_df
    .repartition("procode3")
    .write
    .mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.apc_births")
)
