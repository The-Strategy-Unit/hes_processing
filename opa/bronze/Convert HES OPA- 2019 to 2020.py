# Databricks notebook source
# MAGIC %md
# MAGIC # Convert HES OPA CSV to parquet
# MAGIC
# MAGIC This notebook is to convert the raw csv Outpatient Activity (OPA) Hospital Episode Statistics (HES) files from csv to parquet format.
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

filepath = "/Volumes/su_data/default/hes_raw/opa/"
filename = f"{filepath}/opa_{fyear}"

savepath = f"/Volumes/hes/bronze/raw/opa/fyear={fyear}"

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
        StructField("person_id_deid", StringType(), True),
        StructField("activage", IntegerType(), True),
        StructField("admincat", StringType(), True),
        StructField("apptage", IntegerType(), True),
        StructField("apptage_calc", DecimalType(), True),
        StructField("apptdate", DateType(), True),
        StructField("atentype", StringType(), True),
        StructField("attended", StringType(), True),
        StructField("attendid", StringType(), True),
        StructField("attendkey", StringType(), True),
        StructField("attendkey_flag", StringType(), True),
        StructField("at_gp_practice", StringType(), True),
        StructField("at_residence", StringType(), True),
        StructField("at_treatment", StringType(), True),
        StructField("babyage", StringType(), True),
        StructField("cannet", StringType(), True),
        StructField("canreg", StringType(), True),
        StructField("carersi", StringType(), True),
        StructField("ccg_gp_practice", StringType(), True),
        StructField("ccg_residence", StringType(), True),
        StructField("ccg_responsibility", StringType(), True),
        StructField("ccg_responsibility_origin", StringType(), True),
        StructField("ccg_treatment", StringType(), True),
        StructField("ccg_treatment_origin", StringType(), True),
        StructField("cr_gp_practice", StringType(), True),
        StructField("cr_residence", StringType(), True),
        StructField("cr_treatment", StringType(), True),
        StructField("csnum", StringType(), True),
        StructField("currward", StringType(), True),
        StructField("currward_ons", StringType(), True),
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
        StructField("diag_3_01", StringType(), True),
        StructField("diag_3_02", StringType(), True),
        StructField("diag_3_03", StringType(), True),
        StructField("diag_3_04", StringType(), True),
        StructField("diag_3_05", StringType(), True),
        StructField("diag_3_06", StringType(), True),
        StructField("diag_3_07", StringType(), True),
        StructField("diag_3_08", StringType(), True),
        StructField("diag_3_09", StringType(), True),
        StructField("diag_3_10", StringType(), True),
        StructField("diag_3_11", StringType(), True),
        StructField("diag_3_12", StringType(), True),
        StructField("diag_3_concat", StringType(), True),
        StructField("diag_4_01", StringType(), True),
        StructField("diag_4_02", StringType(), True),
        StructField("diag_4_03", StringType(), True),
        StructField("diag_4_04", StringType(), True),
        StructField("diag_4_05", StringType(), True),
        StructField("diag_4_06", StringType(), True),
        StructField("diag_4_07", StringType(), True),
        StructField("diag_4_08", StringType(), True),
        StructField("diag_4_09", StringType(), True),
        StructField("diag_4_10", StringType(), True),
        StructField("diag_4_11", StringType(), True),
        StructField("diag_4_12", StringType(), True),
        StructField("diag_4_concat", StringType(), True),
        StructField("diag_count", StringType(), True),
        StructField("dnadate", DateType(), True),
        StructField("dob_cfl", StringType(), True),
        StructField("earldatoff", DateType(), True),
        StructField("ethnos", StringType(), True),
        StructField("ethrawl", StringType(), True),
        StructField("firstatt", StringType(), True),
        StructField("fyear", StringType(), True),
        StructField("gortreat", StringType(), True),
        StructField("gpprac", StringType(), True),
        StructField("gppracha", StringType(), True),
        StructField("gppracro", StringType(), True),
        StructField("gpprpct", StringType(), True),
        StructField("gpprstha", StringType(), True),
        StructField("hatreat", StringType(), True),
        StructField("hrgnhs", StringType(), True),
        StructField("hrgnhsvn", StringType(), True),
        StructField("imd04", FloatType(), True),
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
        StructField("imd04_decile", StringType(), True),
        StructField("locclass", StringType(), True),
        StructField("loctype", StringType(), True),
        StructField("lsoa01", StringType(), True),
        StructField("lsoa11", StringType(), True),
        StructField("mainspef", StringType(), True),
        StructField("marstat", StringType(), True),
        StructField("msoa01", StringType(), True),
        StructField("msoa11", StringType(), True),
        StructField("mydob", StringType(), True),
        StructField("newnhsno_check", StringType(), True),
        StructField("nhsnoind", StringType(), True),
        StructField("nodiags", StringType(), True),
        StructField("noprocs", StringType(), True),
        StructField("oacode6", StringType(), True),
        StructField("opcs43", StringType(), True),
        StructField("operstat", StringType(), True),
        StructField("opertn_01", StringType(), True),
        StructField("opertn_02", StringType(), True),
        StructField("opertn_03", StringType(), True),
        StructField("opertn_04", StringType(), True),
        StructField("opertn_05", StringType(), True),
        StructField("opertn_06", StringType(), True),
        StructField("opertn_07", StringType(), True),
        StructField("opertn_08", StringType(), True),
        StructField("opertn_09", StringType(), True),
        StructField("opertn_10", StringType(), True),
        StructField("opertn_11", StringType(), True),
        StructField("opertn_12", StringType(), True),
        StructField("opertn_13", StringType(), True),
        StructField("opertn_14", StringType(), True),
        StructField("opertn_15", StringType(), True),
        StructField("opertn_16", StringType(), True),
        StructField("opertn_17", StringType(), True),
        StructField("opertn_18", StringType(), True),
        StructField("opertn_19", StringType(), True),
        StructField("opertn_20", StringType(), True),
        StructField("opertn_21", StringType(), True),
        StructField("opertn_22", StringType(), True),
        StructField("opertn_23", StringType(), True),
        StructField("opertn_24", StringType(), True),
        StructField("opertn_3_01", StringType(), True),
        StructField("opertn_3_02", StringType(), True),
        StructField("opertn_3_03", StringType(), True),
        StructField("opertn_3_04", StringType(), True),
        StructField("opertn_3_05", StringType(), True),
        StructField("opertn_3_06", StringType(), True),
        StructField("opertn_3_07", StringType(), True),
        StructField("opertn_3_08", StringType(), True),
        StructField("opertn_3_09", StringType(), True),
        StructField("opertn_3_10", StringType(), True),
        StructField("opertn_3_11", StringType(), True),
        StructField("opertn_3_12", StringType(), True),
        StructField("opertn_3_13", StringType(), True),
        StructField("opertn_3_14", StringType(), True),
        StructField("opertn_3_15", StringType(), True),
        StructField("opertn_3_16", StringType(), True),
        StructField("opertn_3_17", StringType(), True),
        StructField("opertn_3_18", StringType(), True),
        StructField("opertn_3_19", StringType(), True),
        StructField("opertn_3_20", StringType(), True),
        StructField("opertn_3_21", StringType(), True),
        StructField("opertn_3_22", StringType(), True),
        StructField("opertn_3_23", StringType(), True),
        StructField("opertn_3_24", StringType(), True),
        StructField("opertn_4_01", StringType(), True),
        StructField("opertn_4_02", StringType(), True),
        StructField("opertn_4_03", StringType(), True),
        StructField("opertn_4_04", StringType(), True),
        StructField("opertn_4_05", StringType(), True),
        StructField("opertn_4_06", StringType(), True),
        StructField("opertn_4_07", StringType(), True),
        StructField("opertn_4_08", StringType(), True),
        StructField("opertn_4_09", StringType(), True),
        StructField("opertn_4_10", StringType(), True),
        StructField("opertn_4_11", StringType(), True),
        StructField("opertn_4_12", StringType(), True),
        StructField("opertn_4_13", StringType(), True),
        StructField("opertn_4_14", StringType(), True),
        StructField("opertn_4_15", StringType(), True),
        StructField("opertn_4_16", StringType(), True),
        StructField("opertn_4_17", StringType(), True),
        StructField("opertn_4_18", StringType(), True),
        StructField("opertn_4_19", StringType(), True),
        StructField("opertn_4_20", StringType(), True),
        StructField("opertn_4_21", StringType(), True),
        StructField("opertn_4_22", StringType(), True),
        StructField("opertn_4_23", StringType(), True),
        StructField("opertn_4_24", StringType(), True),
        StructField("opertn_4_concat", StringType(), True),
        StructField("orgpppid", StringType(), True),
        StructField("outcome", StringType(), True),
        StructField("partyear", IntegerType(), True),
        StructField("pcfound", StringType(), True),
        StructField("pcon", StringType(), True),
        StructField("pconsult", StringType(), True),
        StructField("pcon_ons", StringType(), True),
        StructField("pctcode", StringType(), True),
        StructField("pctcode02", StringType(), True),
        StructField("pctcode06", StringType(), True),
        StructField("pctcode_his", StringType(), True),
        StructField("pctnhs", StringType(), True),
        StructField("pctorig02", StringType(), True),
        StructField("pctorig06", StringType(), True),
        StructField("pctorig_his", StringType(), True),
        StructField("pcttreat", StringType(), True),
        StructField("perstart", DateType(), True),
        StructField("pgpprac", StringType(), True),
        StructField("postdist", StringType(), True),
        StructField("preferer", StringType(), True),
        StructField("preggmp", StringType(), True),
        StructField("primerecp", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("procode", StringType(), True),
        StructField("procode3", StringType(), True),
        StructField("procode5", StringType(), True),
        StructField("procodet", StringType(), True),
        StructField("protype", StringType(), True),
        StructField("purcode", StringType(), True),
        StructField("purstha", StringType(), True),
        StructField("purval", StringType(), True),
        StructField("referorg", StringType(), True),
        StructField("refsourc", StringType(), True),
        StructField("reqdate", DateType(), True),
        StructField("rescty", StringType(), True),
        StructField("rescty_ons", StringType(), True),
        StructField("resgor", StringType(), True),
        StructField("resgor_ons", StringType(), True),
        StructField("resha", StringType(), True),
        StructField("resladst", StringType(), True),
        StructField("resladst_ons", StringType(), True),
        StructField("respct", StringType(), True),
        StructField("respct02", StringType(), True),
        StructField("respct06", StringType(), True),
        StructField("respct_his", StringType(), True),
        StructField("resro", StringType(), True),
        StructField("resstha", StringType(), True),
        StructField("resstha02", StringType(), True),
        StructField("resstha06", StringType(), True),
        StructField("resstha_his", StringType(), True),
        StructField("rotreat", StringType(), True),
        StructField("rttperend", DateType(), True),
        StructField("rttperstart", DateType(), True),
        StructField("rttperstat", StringType(), True),
        StructField("rururb_ind", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("servtype", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("sitetret", StringType(), True),
        StructField("stafftyp", StringType(), True),
        StructField("sthatret", StringType(), True),
        StructField("subdate", DateType(), True),
        StructField("sushrg", StringType(), True),
        StructField("sushrgvers", StringType(), True),
        StructField("suslddate", DateType(), True),
        StructField("susrecid", LongType(), True),
        StructField("susspellid", LongType(), True),
        StructField("tretspef", StringType(), True),
        StructField("waitdays", IntegerType(), True),
        StructField("waiting", IntegerType(), True),
        StructField("wait_ind", StringType(), True),
        StructField("ward91", StringType(), True),
    ]
)

# COMMAND ----------

df = (
    spark.read
    .csv(
        filename,
        header=False,
        schema=csv_schema
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop columns
# MAGIC
# MAGIC drop columns that we don't care about

# COMMAND ----------

df = df.drop("attendid", "fyear", *[
    i
    for i in df.columns
    if i.startswith("diag_3_")
    or i.startswith("diag_4_")
    or i.startswith("opertn_3_")
    or i.startswith("opertn_4_")
    or i.endswith("count")
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save
# MAGIC
# MAGIC Save the data to the data lake

# COMMAND ----------

(
    df.select(*sorted(df.columns))
    .write
    .mode("overwrite")
    .parquet(savepath)
)
