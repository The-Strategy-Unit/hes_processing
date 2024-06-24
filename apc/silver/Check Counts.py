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

# MAGIC %md
# MAGIC
# MAGIC ## Compare to HES Summary Statistics
# MAGIC It is useful to compare what we have loaded to the published Hospital
# MAGIC Admitted Patient Care Activity files [digital.nhs.uk](https://digital.nhs.uk/data-and-information/publications/statistical/hospital-admitted-patient-care-activity)
# MAGIC
# MAGIC Note, that these files suppress data by replacing counts less than 5
# MAGIC with a *, and then rounding all other values to the nearest 5.

# COMMAND ----------

admimeth = F.col("admimeth")

fce = (
    spark.read.table("hes.silver.apc")
    .filter(F.col("fce") == True)
    .filter(F.col("epistat") == "3")
    .filter(F.col("classpat").isin(["1", "2", "5"]))
)

fae_fce = (
    fce
    .groupBy(["fyear", "procode3"])
    .agg(
        F.count("fce").alias("fce"),
        F.sum("fae").alias("fae")
    )
)

fae_by_group = (
    fce
    .withColumn(
        "admission_group",
        F
            .when(admimeth.rlike("^2"), "fae_emergency")
            .when(admimeth.rlike("^1"), "fae_elective")
            .otherwise("fae_other")
    )
    .groupBy(["fyear", "procode3"])
    .pivot("admission_group")
    .count()
    .sort(["fyear", "procode3"])
    .na.fill(0)
)

(
    fae_fce
    .join(fae_by_group, ["fyear", "procode3"], "left")
    .sort(["procode3", "fyear"])
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check our last episode in spell column
# MAGIC
# MAGIC The following produces counts by last episode in spell. It should be
# MAGIC close to the prior counts, but those counts are based on the end date of
# MAGIC the admitting episode. This is based on the end date of the discharging
# MAGIC episode.

# COMMAND ----------

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
).display()
