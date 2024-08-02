# Databricks notebook source
# MAGIC %md
# MAGIC # Convert HES OPA to parquet
# MAGIC
# MAGIC This notebook is to convert the raw parquet Outpatient Activity (OPA) Hospital Episode Statistics (HES) files supplied by NHS England.
# MAGIC
# MAGIC Once this has run, the parquet files will form the bronze level data, and the parquet files could be removed.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from databricks.sdk.runtime import *

from functools import reduce
import os

# COMMAND ----------

year = int(dbutils.widgets.get("year"))
fyear = year * 100 + ((year + 1) % 100)

filepath = "abfss://nhse-nhp-data@sudata.dfs.core.windows.net/NHP_HES/NHP_HESOPA/"

savepath = f"/Volumes/hes/bronze/raw/opa/fyear={fyear}"

# COMMAND ----------

if os.path.exists(savepath):
    dbutils.notebook.exit("data already exists: skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data

# COMMAND ----------

df = (
    spark.read
    .parquet(filepath)
    .filter(F.col("fyear") == (fyear % 10000))
)
# convert all column names to lower case
df = df.select([F.col(c).alias(c.lower()) for c in df.columns])


# COMMAND ----------

df = (
    df
    .withColumnRenamed("apptdate_derived", "apptdate")
    .withColumnRenamed("dnadate_derived", "dnadate")
    .withColumnRenamed("rttperend", "rttperend")
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
        "apptage",
        "partyear"
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
    .parquet(savepath)
)
