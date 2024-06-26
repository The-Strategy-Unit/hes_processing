# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from databricks.sdk.runtime import *

from fix_icd10_or_opcs4 import fix_icd10_or_opcs4

# COMMAND ----------

df = spark.read.table("hes.bronze.opa")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove normalised columns
# MAGIC
# MAGIC These columns are normalised in other tasks in the workflow.

# COMMAND ----------

df = df.drop(
    *(
        [
            f"diag_{i:02}" for i in range(1, 13)
        ] + [
            f"{c}_{i:02}"
            for i in range(1, 25)
            for c in ["opdate", "opertn"]
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop the IMD column
# MAGIC
# MAGIC The imd columns are fixed as imd04 - it would be more useful to join to the relevant imd files later on as needed.

# COMMAND ----------

df = df.drop(*[i for i in df.columns if i.startswith("imd")])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

(
    df
    .repartition("procode3")
    .write
    .mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.opa")
)
