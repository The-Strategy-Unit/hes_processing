# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

from databricks.sdk.runtime import *

from fix_icd10_or_opcs4 import fix_icd10_or_opcs4

# COMMAND ----------

df = spark.read.table("hes.bronze.apc")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Fix diagnosis columns
# MAGIC
# MAGIC Some of the diagnosis fields contain additional characters: remove these.

# COMMAND ----------

df = fix_icd10_or_opcs4(df, "alcdiag")
df = fix_icd10_or_opcs4(df, "cause")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove normalised columns
# MAGIC
# MAGIC These columns are normalised in other tasks in the workflow.

# COMMAND ----------

df = df.drop(
    *(
        [
            f"{c}_{i}"
            for i in range(1, 10)
            for c in [
                "acpdisp",
                "acpdqind",
                "acpend",
                "acploc",
                "acpn",
                "acpout",
                "acpplan",
                "acpsour",
                "acpspef",
                "acpstar",
                "biresus",
                "birordr",
                "birstat",
                "birweit",
                "delmeth",
                "delplac",
                "delstat",
                "depdays",
                "gestat",
                "intdays",
                "orgsup",
                "sexbaby"
            ]
        ] + [
            f"diag_{i:02}" for i in range(1, 21)
        ] + [
            f"{c}_{i:02}"
            for i in range(1, 25)
            for c in ["opdate", "opertn"]
        ] +
        # drop other columns
        ["alcdiag_4", "cause_3", "cause_4", "diag3_01", "diag4_01", "opertn3_01"]
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
# MAGIC ## Create last episode in spell column
# MAGIC
# MAGIC Uses the methodology in [Methodology to create provider and CIP spells from HES APC data](https://files.digital.nhs.uk/B6/4A484B/Methodology%20to%20create%20provider%20and%20CIP%20spells%20from%20HES%20APC%20data%20v2.pdf)
# MAGIC
# MAGIC > Episodes that have the same `TOKEN_PERSON_ID`, `ADMIDATE`, `PROCODET_MAPPED` and `PROVSPNOPS` are considered to be in the same provider spell.
# MAGIC > Regular attender episodes (`CLASSPAT` = `"3"` and `"4"`) are considered as separate units of care that should not be linked to other episodes and therefore are excluded from the episode ordering criteria shown below – they form single episode provider spells.
# MAGIC >
# MAGIC > Episodes within a provider spell are sorted using the following criteria:
# MAGIC > 1. EPISTART
# MAGIC > 2. EPIORDER
# MAGIC > 3. EPIEND
# MAGIC > 4. EPIKEY
# MAGIC >
# MAGIC > The order of episodes within the spell is indicated by a derived field called P_SPELL_EPIORDER.
# MAGIC > In most cases this field should match the provider submitter episode order (EPIORDER) but in a small number of cases data quality issues have caused this to be different.
# MAGIC >
# MAGIC > ...
# MAGIC >
# MAGIC > These episodes are flagged using the derived field `P_SPELL_LAST_EPISODE` = `"Y"`.
# MAGIC > This flag is applied only on "closed spells" (i.e. spells with an episode containing a valid discharge date) on the episode with the highest `P_SPELL_EPIORDER`
# MAGIC

# COMMAND ----------

w = (
    Window
    .partitionBy(["susspellid"])
    .orderBy(F.desc("epistart"), F.desc("epiorder"), F.desc("epiend"), F.desc("epikey"))
)

last_episode_in_spell = (
    df
    .filter(F.col("epistat") == 3)
    .filter(F.col("admidate").isNotNull())
    .filter(F.col("dismeth") != "8")
    .filter(F.col("disdate").isNotNull())
    .filter(F.col("susspellid") != "-1")
    .filter(F.col("susspellid").isNotNull())
    .withColumn(
        "p_rev_spell_epiorder",
        F.row_number().over(w)
    )
    .filter(F.col("p_rev_spell_epiorder") == 1)
    .select("epikey")
    .withColumn(
        "last_episode_in_spell",
        F.lit(True)
    )
)
    
df = (
    df
    .join(last_episode_in_spell, "epikey", "left")
    .na.fill(False, ["last_episode_in_spell"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

(
    df
    .repartition("procode3")
    .write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .partitionBy(["fyear", "procode3", "last_episode_in_spell"])
    .saveAsTable("hes.silver.apc")
)
