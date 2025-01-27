# Databricks notebook source
# MAGIC %md
# MAGIC # Convert HES AE CSV to parquet
# MAGIC
# MAGIC This notebook is to convert the raw csv Accident and Emergency (AE) Hospital Episode Statistics (HES) files from csv to parquet format.
# MAGIC
# MAGIC Once this has run, the parquet files will form the bronze level data, and the csv files can be removed.
# MAGIC
# MAGIC Change the year in the widget at the top of the notebook to run for different years.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from databricks.sdk.runtime import *

import os

# COMMAND ----------

year = int(dbutils.widgets.get("year"))
fyear = year * 100 + ((year + 1) % 100)

filepath = "/Volumes/su_data/default/hes_raw/ae/"
filename = f"{filepath}/ae_{fyear}"
mpsid_file = f"{filepath}/ae_{fyear}_mpsid.parquet"

savepath = f"/Volumes/hes/bronze/raw/ae/fyear={fyear}"

# COMMAND ----------

if os.path.exists(savepath):
    dbutils.notebook.exit("data already exists: skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC define the schema of the csv to load

# COMMAND ----------

csv_schema = StructType(
    [
        StructField("activage", IntegerType(), True),
        StructField("aearrivalmode", StringType(), True),
        StructField("aeattend_exc_planned", StringType(), True),
        StructField("aeattendcat", StringType(), True),
        StructField("aeattenddisp", StringType(), True),
        StructField("aedepttype", StringType(), True),
        StructField("aeincloctype", StringType(), True),
        StructField("aekey", StringType(), True),
        StructField("aekey_flag", IntegerType(), True),
        StructField("aepatgroup", StringType(), True),
        StructField("aerefsource", StringType(), True),
        StructField("appdate", DateType(), True),
        StructField("arrivalage", IntegerType(), True),
        StructField("arrivalage_calc", FloatType(), True),
        StructField("arrivaldate", DateType(), True),
        StructField("arrivaltime", IntegerType(), True),
        StructField("at_gp_practice", StringType(), True),
        StructField("at_residence", StringType(), True),
        StructField("at_treatment", StringType(), True),
        StructField("cannet", StringType(), True),
        StructField("canreg", StringType(), True),
        StructField("carersi", StringType(), True),
        StructField("ccg_gp_practice", StringType(), True),
        StructField("ccg_residence", StringType(), True),
        StructField("ccg_responsibility", StringType(), True),
        StructField("ccg_responsibility_origin", StringType(), True),
        StructField("ccg_treatment", StringType(), True),
        StructField("ccg_treatment_origin", StringType(), True),
        StructField("cdsextdate", DateType(), True),
        StructField("cdsuniqueid", StringType(), True),
        StructField("cdsverprotid", StringType(), True),
        StructField("concldur", IntegerType(), True),
        StructField("concltime", IntegerType(), True),
        StructField("cr_gp_practice", StringType(), True),
        StructField("cr_residence", StringType(), True),
        StructField("cr_treatment", StringType(), True),
        StructField("csnum", StringType(), True),
        StructField("currward", StringType(), True),
        StructField("currward_ons", StringType(), True),
        StructField("depdur", IntegerType(), True),
        StructField("deptime", IntegerType(), True),
        StructField("diag_01", StringType(), True),
        StructField("diag_02", StringType(), True),
        StructField("diag_03", StringType(), True),
        StructField("diag_04", StringType(), True),
        StructField("diag_05", StringType(), True),
        StructField("diag_06", StringType(), True),
        StructField("diag_07", StringType(), True),
        StructField("diag_08", StringType(), True),
        StructField("diag_09", StringType(), True),
        StructField("diag_10", StringType(), True),
        StructField("diag_11", StringType(), True),
        StructField("diag_12", StringType(), True),
        StructField("diaga_01", StringType(), True),
        StructField("diaga_02", StringType(), True),
        StructField("diaga_03", StringType(), True),
        StructField("diaga_04", StringType(), True),
        StructField("diaga_05", StringType(), True),
        StructField("diaga_06", StringType(), True),
        StructField("diaga_07", StringType(), True),
        StructField("diaga_08", StringType(), True),
        StructField("diaga_09", StringType(), True),
        StructField("diaga_10", StringType(), True),
        StructField("diaga_11", StringType(), True),
        StructField("diaga_12", StringType(), True),
        StructField("diags_01", StringType(), True),
        StructField("diags_02", StringType(), True),
        StructField("diags_03", StringType(), True),
        StructField("diags_04", StringType(), True),
        StructField("diags_05", StringType(), True),
        StructField("diags_06", StringType(), True),
        StructField("diags_07", StringType(), True),
        StructField("diags_08", StringType(), True),
        StructField("diags_09", StringType(), True),
        StructField("diags_10", StringType(), True),
        StructField("diags_11", StringType(), True),
        StructField("diags_12", StringType(), True),
        StructField("diagscheme", StringType(), True),
        StructField("domproc", StringType(), True),
        StructField("encrypted_hesid", StringType(), True),
        StructField("epikey", StringType(), True),
        StructField("ethnos", StringType(), True),
        StructField("fyear", StringType(), True),
        StructField("gortreat", StringType(), True),
        StructField("gpprac", StringType(), True),
        StructField("gpprpct", StringType(), True),
        StructField("gpprstha", StringType(), True),
        StructField("hatreat", StringType(), True),
        StructField("imd04", FloatType(), True),
        StructField("imd04_decile", StringType(), True),
        StructField("imd04c", FloatType(), True),
        StructField("imd04ed", FloatType(), True),
        StructField("imd04em", FloatType(), True),
        StructField("imd04hd", FloatType(), True),
        StructField("imd04hs", FloatType(), True),
        StructField("imd04i", FloatType(), True),
        StructField("imd04ia", FloatType(), True),
        StructField("imd04ic", FloatType(), True),
        StructField("imd04le", FloatType(), True),
        StructField("imd04rk", FloatType(), True),
        StructField("initdur", IntegerType(), True),
        StructField("inittime", IntegerType(), True),
        StructField("invest_01", StringType(), True),
        StructField("invest_02", StringType(), True),
        StructField("invest_03", StringType(), True),
        StructField("invest_04", StringType(), True),
        StructField("invest_05", StringType(), True),
        StructField("invest_06", StringType(), True),
        StructField("invest_07", StringType(), True),
        StructField("invest_08", StringType(), True),
        StructField("invest_09", StringType(), True),
        StructField("invest_10", StringType(), True),
        StructField("invest_11", StringType(), True),
        StructField("invest_12", StringType(), True),
        StructField("invest2_01", StringType(), True),
        StructField("invest2_02", StringType(), True),
        StructField("invest2_03", StringType(), True),
        StructField("invest2_04", StringType(), True),
        StructField("invest2_05", StringType(), True),
        StructField("invest2_06", StringType(), True),
        StructField("invest2_07", StringType(), True),
        StructField("invest2_08", StringType(), True),
        StructField("invest2_09", StringType(), True),
        StructField("invest2_10", StringType(), True),
        StructField("invest2_11", StringType(), True),
        StructField("invest2_12", StringType(), True),
        StructField("lsoa01", StringType(), True),
        StructField("lsoa11", StringType(), True),
        StructField("msoa01", StringType(), True),
        StructField("msoa11", StringType(), True),
        StructField("mydob", StringType(), True),
        StructField("newnhsno_check", StringType(), True),
        StructField("nhsnoind", StringType(), True),
        StructField("nodiags", IntegerType(), True),
        StructField("noinvests", IntegerType(), True),
        StructField("notreats", IntegerType(), True),
        StructField("oacode6", StringType(), True),
        StructField("orgpppid", StringType(), True),
        StructField("partyear", StringType(), True),
        StructField("pcfound", StringType(), True),
        StructField("pcon", StringType(), True),
        StructField("pcon_ons", StringType(), True),
        StructField("pctcode06", StringType(), True),
        StructField("pcttreat", StringType(), True),
        StructField("perend", StringType(), True),
        StructField("postdist", StringType(), True),
        StructField("preggmp", StringType(), True),
        StructField("procode", StringType(), True),
        StructField("procode3", StringType(), True),
        StructField("procode5", StringType(), True),
        StructField("procodet", StringType(), True),
        StructField("protype", StringType(), True),
        StructField("purcode", StringType(), True),
        StructField("purval", StringType(), True),
        StructField("rescty", StringType(), True),
        StructField("rescty_ons", StringType(), True),
        StructField("resgor", StringType(), True),
        StructField("resgor_ons", StringType(), True),
        StructField("resha", StringType(), True),
        StructField("resladst", StringType(), True),
        StructField("resladst_ons", StringType(), True),
        StructField("respct_his", StringType(), True),
        StructField("respct06", StringType(), True),
        StructField("resro", StringType(), True),
        StructField("resstha_his", StringType(), True),
        StructField("resstha06", StringType(), True),
        StructField("rotreat", StringType(), True),
        StructField("rttperend", StringType(), True),
        StructField("rttperstart", StringType(), True),
        StructField("rttperstat", StringType(), True),
        StructField("rururb_ind", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("sthatret", StringType(), True),
        StructField("subdate", DateType(), True),
        StructField("sushrg", StringType(), True),
        StructField("sushrginfo", StringType(), True),
        StructField("sushrgverinfo", StringType(), True),
        StructField("sushrgvers", StringType(), True),
        StructField("suslddate", DateType(), True),
        StructField("susrecid", StringType(), True),
        StructField("susspellid", StringType(), True),
        StructField("treat_01", StringType(), True),
        StructField("treat_02", StringType(), True),
        StructField("treat_03", StringType(), True),
        StructField("treat_04", StringType(), True),
        StructField("treat_05", StringType(), True),
        StructField("treat_06", StringType(), True),
        StructField("treat_07", StringType(), True),
        StructField("treat_08", StringType(), True),
        StructField("treat_09", StringType(), True),
        StructField("treat_10", StringType(), True),
        StructField("treat_11", StringType(), True),
        StructField("treat_12", StringType(), True),
        StructField("tretdur", StringType(), True),
        StructField("trettime", StringType(), True),
        StructField("waitdays", IntegerType(), True),
        StructField("ward91", StringType(), True)
    ]
)

# COMMAND ----------

df = (
    spark.read
    .csv(
        filename,
        header=False,
        schema=csv_schema,
        sep="|"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add new person id
# MAGIC

# COMMAND ----------

mpsid = (
    spark.read.parquet(mpsid_file)
    .withColumnRenamed("tokenid", "person_id_deid")
)

df = df.join(mpsid, "aekey", "left")


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
