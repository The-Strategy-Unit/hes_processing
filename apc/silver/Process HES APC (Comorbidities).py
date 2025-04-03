# Databricks notebook source
import json
from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# COMMAND ----------


def open_comorbidity_file(file, scores):
    with open(
        f"/Volumes/strategyunit/reference/files/{file}_comorbidity_scores.json", "r"
    ) as f:
        return spark.createDataFrame(
            [
                {
                    "group": v.get("trumps", k),
                    "score_type": s,
                    "score": v["score"][s],
                    "code": c,
                }
                for k, v in json.load(f).items()
                for c in v["codes"]
                for s in scores
            ]
        ).withColumn("type", F.lit(file))


comorbidities = reduce(
    DataFrame.unionByName,
    [
        open_comorbidity_file(*i)
        for i in [("charlson", ["charlson", "quan"]), ("elixhauser", ["vw", "swiss"])]
    ],
)

# COMMAND ----------

diags = spark.read.table("hes.silver.apc_diagnoses")

# COMMAND ----------

comorbidities_expanded = (
    diags.select(F.col("diagnosis"))
    .distinct()
    .join(comorbidities, F.expr("diagnosis LIKE concat(code, '%')"))
    .persist()
)

comorbidities_expanded.display()

# COMMAND ----------

comorbidities_df = (
    diags.join(comorbidities_expanded, on=["diagnosis"])
    .groupBy("procode3", "fyear", "epikey", "type", "group", "score_type")
    .agg(F.max("score").alias("score"))
    # handle trumps
    .groupBy("procode3", "fyear", "epikey", "type", "score_type")
    .agg(F.sum("score").alias("score"))
    .orderBy("type", "score_type")
)

# COMMAND ----------

(
    comorbidities_df.repartition("procode3")
    .write.mode("overwrite")
    .partitionBy(["fyear", "procode3", "type", "score_type"])
    .saveAsTable("hes.silver.apc_comorbidities")
)
