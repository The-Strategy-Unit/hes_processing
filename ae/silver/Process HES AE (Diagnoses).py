# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from databricks.sdk.runtime import *

# COMMAND ----------

df = spark.read.table("hes.bronze.aae")

# COMMAND ----------

to_melt = [f"diag_{i:02}" for i in range(1, 12)]
melt_str = ','.join([f"'{c}', `{c}`" for c in to_melt])

stack_expr = F.expr(f"stack({len(to_melt)}, {melt_str}) as (diag_order, diagnosis)")

# COMMAND ----------

diag_df = (
    df
    .select(
        "aekey",
        "fyear",
        "procode3",
        stack_expr
    )
    .filter(F.col("diagnosis").isNotNull())
    .withColumn(
        "diag_order",
        F.substring(F.col("diag_order"), 6, 2).cast(IntegerType())
    )
)


# COMMAND ----------

(
    diag_df
    .repartition("procode3")
    .write
    .mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.aae_diagnoses")
)
