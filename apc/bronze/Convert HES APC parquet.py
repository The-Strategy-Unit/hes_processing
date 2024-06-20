# Databricks notebook source
# MAGIC %md
# MAGIC # Convert HES APC CSV to parquet
# MAGIC
# MAGIC This notebook is to convert the raw parquet Admitted Patient Care (APC) Hospital Episode Statistics (HES) files supplied by NHS England.
# MAGIC
# MAGIC Once this has run, the parquet files will form the bronze level data, and the parquet files could be removed.

# COMMAND ----------

from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import functions as F
from databricks.sdk.runtime import *

# COMMAND ----------

year = int(dbutils.widgets.get("year"))
fyear = year * 100 + ((year + 1) % 100)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data

# COMMAND ----------

df = (
    spark.read
    .parquet("/Volumes/su_data/default/nhp_hes_apc/")
    .filter(F.col("fyear") == (fyear % 10000))
)
# convert all column names to lower case
df = df.select([F.col(c).alias(c.lower()) for c in df.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename Columns

# COMMAND ----------

df = (
    df
    .withColumnRenamed("acscflag_derived", "acscflag")
    .withColumnRenamed("acscflag_derived", "acscflag")
    .withColumnRenamed("admidate_derived", "admidate")
    .withColumnRenamed("disdate_derived", "disdate")
    .withColumnRenamed("disreadydate_derived", "disreadydate")
    .withColumnRenamed("elecdate_derived", "elecdate")
    .withColumnRenamed("epiend_derived", "epiend")
    .withColumnRenamed("epistart_derived", "epistart")
    .withColumnRenamed("rttperend_derived", "rttperend")
    .withColumnRenamed("rttperstart_derived", "rttperstart")
    .withColumnRenamed("token_person_id", "person_id_deid")
    .drop("fileid", "period", "subperiod", "sourcesystem", "fyear")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert columns to integers

# COMMAND ----------


# convert some columns
df = reduce(
    lambda x, y: x.withColumn(y, F.col(y).cast(IntegerType())),
    [
        "activage",
        "admiage",
        "anagest",
        "delonset",
        "epistat",
        "epitype",
        "fae",
        "fae_emergency",
        "fce",
        "fde",
        "partyear",
        "spelbgin"
    ],
    df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert columns to floats

# COMMAND ----------

df = reduce(
    lambda x, y: x.withColumn(y, F.col(y).cast(FloatType())),
    ["imd04", "imd04c", "imd04ed", "imd04em", "imd04hd", "imd04hs", "imd04i", "imd04ia", "imd04ic", "imd04le", "imd04rk"],
    df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save
# MAGIC
# MAGIC Save the data to the data lake

# COMMAND ----------

(
    df.select(*sorted(df.columns))
    .repartition(32)
    .write
    .mode("overwrite")
    .parquet(f"/Volumes/hes/bronze/raw/apc/fyear={fyear}")
)
