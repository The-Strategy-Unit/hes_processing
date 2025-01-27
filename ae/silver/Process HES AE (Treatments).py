# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from databricks.sdk.runtime import *

# COMMAND ----------

df = spark.read.table("hes.bronze.aae")

# COMMAND ----------

to_melt = [f"treat_{i:02}" for i in range(1, 12)]
melt_str = ','.join([f"'{c}', `{c}`" for c in to_melt])

stack_expr = F.expr(f"stack({len(to_melt)}, {melt_str}) as (treatment_order, treatment)")

# COMMAND ----------

treat_df = (
    df
    .select(
        "aekey",
        "fyear",
        "procode3",
        stack_expr
    )
    .filter(F.col("treatment").isNotNull())
    .withColumn(
        "treatment_order",
        F.substring(F.col("treatment_order"), 7, 2).cast(IntegerType())
    )
)


# COMMAND ----------

(
    treat_df
    .repartition("procode3")
    .write
    .mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.aae_treatments")
)
