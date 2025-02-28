# Databricks notebook source
from databricks.sdk.runtime import *
from fix_icd10_or_opcs4 import fix_icd10_or_opcs4
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

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
        [f"diag_{i:02}" for i in range(1, 13)]
        + [f"{c}_{i:02}" for i in range(1, 25) for c in ["opdate", "opertn"]]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recreate the IMD decile
# MAGIC

# COMMAND ----------

# IMD04 on activity up to and including 2006-07
# IMD07 on activity between 2007-08 and 2009-10
# IMD10 on activity from 2010-11 and M10 2022-23
# IMD19 from M11 2022-23

imdrk_to_ntile = (
    df.select("fyear", "imd04rk")
    .filter(F.col("imd04rk").isNotNull())
    .distinct()
    .withColumn(
        "imd_decile", F.ntile(10).over(Window.partitionBy("fyear").orderBy("imd04rk"))
    )
    .withColumn(
        "imd_quintile",
        F.ntile(5).over(Window.partitionBy("fyear").orderBy("imd04rk")),
    )
)

df = (
    df.drop("imd04_decile")
    .join(imdrk_to_ntile, ["fyear", "imd04rk"], "left")
    .withColumn(
        "imd_version",
        F.when(F.col("imd04rk").isNull(), F.lit(None).cast("string")).when(
            F.col("fyear") >= 202324, F.lit("IMD19")
        )
        # change happened in M10 2022/23, e.g. January 2023
        .when(F.year(F.col("admidate")) == 2023, F.lit("IMD19"))
        .when(F.col("fyear") >= 201011, F.lit("IMD10"))
        .when(F.col("fyear") >= 200708, F.lit("IMD07"))
        .otherwise(F.lit("IMD04")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

(
    df.select(*sorted(df.columns))
    .repartition("procode3")
    .write.mode("overwrite")
    .partitionBy(["fyear", "procode3"])
    .saveAsTable("hes.silver.opa")
)
