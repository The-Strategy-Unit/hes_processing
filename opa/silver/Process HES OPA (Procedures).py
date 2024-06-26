# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

from databricks.sdk.runtime import *

from fix_icd10_or_opcs4 import fix_icd10_or_opcs4

# COMMAND ----------

df = spark.read.table("hes.bronze.opa")

# COMMAND ----------

dfs = [
    (
        df
        .select(
            "attendkey",
            "fyear",
            "procode3",
            F.col(f"opertn_{i:02}").alias("procedure_code")
        )
        .filter(F.col(f"opertn_{i:02}").isNotNull())
        .withColumn("procedure_order", F.lit(i))
    )
    for i in range(1, 25)
]

opertn_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)    

# COMMAND ----------

opertn_df = fix_icd10_or_opcs4(
    opertn_df
        .filter( F.col("procedure_code").rlike("^[A-Z]\\d{3}$"))
        .filter(~F.col("procedure_code").rlike("^X99")),
    "procedure_code"
)


# COMMAND ----------

(
    opertn_df
    .repartition("procode3")
    .write
    .mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.opa_procedures")
)
