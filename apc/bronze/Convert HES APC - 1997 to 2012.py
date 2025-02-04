# Databricks notebook source
# MAGIC %md
# MAGIC # Convert HES APC CSV to parquet
# MAGIC
# MAGIC This notebook is to convert the raw csv Admitted Patient Care (APC) Hospital Episode Statistics (HES) files from csv to parquet format.
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

filepath = "/Volumes/su_data/default/hes_raw/apc/"
filename = f"{filepath}/apc_{fyear}"
mpsid_file = f"{filepath}/apc_{fyear}_mpsid.parquet"

# COMMAND ----------

try:
    previously_run = (
        spark.read.table("hes.bronze.apc")
        .filter(F.col("fyear") == fyear)
        .count()
    ) > 1
    if previously_run:
        dbutils.notebook.exit("data already exists: skipping")
except:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC define the schema of the csv to load

# COMMAND ----------

csv_schema = StructType(
    [
        StructField("category", StringType(), True),
        StructField("admincat", StringType(), True),
        StructField("endage", IntegerType(), True),
        StructField("startage", IntegerType(), True),
        StructField("neodur", IntegerType(), True),
        StructField("mydob", StringType(), True),
        StructField("dob_cfl", IntegerType(), True),
        StructField("ethnos", StringType(), True),
        StructField("encrypted_hesid", StringType(), True),
        StructField("postdist", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("admidate", DateType(), True),
        StructField("adm_cfl", IntegerType(), True),
        StructField("elecdate", DateType(), True),
        StructField("elec_cfl", IntegerType(), True),
        StructField("admimeth", StringType(), True),
        StructField("admisorc", StringType(), True),
        StructField("firstreg", StringType(), True),
        StructField("elecdur", IntegerType(), True),
        StructField("disdate", DateType(), True),
        StructField("dis_cfl", IntegerType(), True),
        StructField("disdest", StringType(), True),
        StructField("dismeth", StringType(), True),
        StructField("bedyear", IntegerType(), True),
        StructField("spelbgin", IntegerType(), True),
        StructField("epiend", DateType(), True),
        StructField("epistart", DateType(), True),
        StructField("speldur", IntegerType(), True),
        StructField("spelend", StringType(), True),
        StructField("epidur", IntegerType(), True),
        StructField("epiorder", IntegerType(), True),
        StructField("epie_cfl", IntegerType(), True),
        StructField("epis_cfl", IntegerType(), True),
        StructField("epistat", IntegerType(), True),
        StructField("epitype", IntegerType(), True),
        StructField("provspno", LongType(), True),
        StructField("wardstrt", StringType(), True),
        StructField("disreadydate", DateType(), True),
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
        StructField("diag_13", StringType(), True),
        StructField("diag_14", StringType(), True),
        StructField("diag_15", StringType(), True),
        StructField("diag_16", StringType(), True),
        StructField("diag_17", StringType(), True),
        StructField("diag_18", StringType(), True),
        StructField("diag_19", StringType(), True),
        StructField("diag_20", StringType(), True),
        StructField("diag3_01", StringType(), True),
        StructField("diag4_01", StringType(), True),
        StructField("cause", StringType(), True),
        StructField("cause4", StringType(), True),
        StructField("cause3", StringType(), True),
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
        StructField("opertn3_01", StringType(), True),
        StructField("opdate_01", DateType(), True),
        StructField("opdate_02", DateType(), True),
        StructField("opdate_03", DateType(), True),
        StructField("opdate_04", DateType(), True),
        StructField("opdate_05", DateType(), True),
        StructField("opdate_06", DateType(), True),
        StructField("opdate_07", DateType(), True),
        StructField("opdate_08", DateType(), True),
        StructField("opdate_09", DateType(), True),
        StructField("opdate_10", DateType(), True),
        StructField("opdate_11", DateType(), True),
        StructField("opdate_12", DateType(), True),
        StructField("opdate_13", DateType(), True),
        StructField("opdate_14", DateType(), True),
        StructField("opdate_15", DateType(), True),
        StructField("opdate_16", DateType(), True),
        StructField("opdate_17", DateType(), True),
        StructField("opdate_18", DateType(), True),
        StructField("opdate_19", DateType(), True),
        StructField("opdate_20", DateType(), True),
        StructField("opdate_21", DateType(), True),
        StructField("opdate_22", DateType(), True),
        StructField("opdate_23", DateType(), True),
        StructField("opdate_24", DateType(), True),
        StructField("operstat", StringType(), True),
        StructField("posopdur", IntegerType(), True),
        StructField("preopdur", IntegerType(), True),
        StructField("classpat", StringType(), True),
        StructField("intmanig", StringType(), True),
        StructField("mainspef", StringType(), True),
        StructField("tretspef", StringType(), True),
        StructField("domproc", StringType(), True),
        StructField("hrglate35", StringType(), True),
        StructField("hrgnhs", StringType(), True),
        StructField("hrgnhsvn", StringType(), True),
        StructField("suscorehrg", StringType(), True),
        StructField("sushrg", StringType(), True),
        StructField("sushrgvers", StringType(), True),
        StructField("susspellid", StringType(), True),
        StructField("purcode", StringType(), True),
        StructField("purval", StringType(), True),
        StructField("purro", StringType(), True),
        StructField("purstha", StringType(), True),
        StructField("csnum", StringType(), True),
        StructField("gppracha", StringType(), True),
        StructField("pcgcode", StringType(), True),
        StructField("pctcode", StringType(), True),
        StructField("pctcode06", StringType(), True),
        StructField("gpprpct", StringType(), True),
        StructField("procode", StringType(), True),
        StructField("procode3", StringType(), True),
        StructField("procodet", StringType(), True),
        StructField("sitetret", StringType(), True),
        StructField("protype", StringType(), True),
        StructField("gppracro", StringType(), True),
        StructField("gpprstha", StringType(), True),
        StructField("oacode6", StringType(), True),
        StructField("rescty", StringType(), True),
        StructField("resladst", StringType(), True),
        StructField("resladst_currward", StringType(), True),
        StructField("ward91", StringType(), True),
        StructField("resgor", StringType(), True),
        StructField("gortreat", StringType(), True),
        StructField("resha", StringType(), True),
        StructField("hatreat", StringType(), True),
        StructField("pctnhs", StringType(), True),
        StructField("respct", StringType(), True),
        StructField("respct06", StringType(), True),
        StructField("resstha", StringType(), True),
        StructField("resstha06", StringType(), True),
        StructField("pcttreat", StringType(), True),
        StructField("rotreat", StringType(), True),
        StructField("resro", StringType(), True),
        StructField("sthatret", StringType(), True),
        StructField("lsoa01", StringType(), True),
        StructField("msoa01", StringType(), True),
        StructField("rururb_ind", StringType(), True),
        StructField("imd04c", FloatType(), True),
        StructField("imd04ed", FloatType(), True),
        StructField("imd04em", FloatType(), True),
        StructField("imd04hd", FloatType(), True),
        StructField("imd04hs", FloatType(), True),
        StructField("imd04i", FloatType(), True),
        StructField("imd04ia", FloatType(), True),
        StructField("imd04ic", FloatType(), True),
        StructField("imd04le", FloatType(), True),
        StructField("imd04", FloatType(), True),
        StructField("imd04rk", FloatType(), True),
        StructField("imd04_decile", StringType(), True),
        StructField("gpprac", StringType(), True),
        StructField("pconsult", StringType(), True),
        StructField("preggmp", StringType(), True),
        StructField("preferer", StringType(), True),
        StructField("referorg", StringType(), True),
        StructField("acploc_1", StringType(), True),
        StructField("acploc_2", StringType(), True),
        StructField("acploc_3", StringType(), True),
        StructField("acploc_4", StringType(), True),
        StructField("acploc_5", StringType(), True),
        StructField("acploc_6", StringType(), True),
        StructField("acploc_7", StringType(), True),
        StructField("acploc_8", StringType(), True),
        StructField("acploc_9", StringType(), True),
        StructField("acpdqind_1", StringType(), True),
        StructField("acpdqind_2", StringType(), True),
        StructField("acpdqind_3", StringType(), True),
        StructField("acpdqind_4", StringType(), True),
        StructField("acpdqind_5", StringType(), True),
        StructField("acpdqind_6", StringType(), True),
        StructField("acpdqind_7", StringType(), True),
        StructField("acpdqind_8", StringType(), True),
        StructField("acpdqind_9", StringType(), True),
        StructField("acpdisp_1", StringType(), True),
        StructField("acpdisp_2", StringType(), True),
        StructField("acpdisp_3", StringType(), True),
        StructField("acpdisp_4", StringType(), True),
        StructField("acpdisp_5", StringType(), True),
        StructField("acpdisp_6", StringType(), True),
        StructField("acpdisp_7", StringType(), True),
        StructField("acpdisp_8", StringType(), True),
        StructField("acpdisp_9", StringType(), True),
        StructField("acpend_1", DateType(), True),
        StructField("acpend_2", DateType(), True),
        StructField("acpend_3", DateType(), True),
        StructField("acpend_4", DateType(), True),
        StructField("acpend_5", DateType(), True),
        StructField("acpend_6", DateType(), True),
        StructField("acpend_7", DateType(), True),
        StructField("acpend_8", DateType(), True),
        StructField("acpend_9", DateType(), True),
        StructField("acpplan_1", StringType(), True),
        StructField("acpplan_2", StringType(), True),
        StructField("acpplan_3", StringType(), True),
        StructField("acpplan_4", StringType(), True),
        StructField("acpplan_5", StringType(), True),
        StructField("acpplan_6", StringType(), True),
        StructField("acpplan_7", StringType(), True),
        StructField("acpplan_8", StringType(), True),
        StructField("acpplan_9", StringType(), True),
        StructField("acpn_1", IntegerType(), True),
        StructField("acpn_2", IntegerType(), True),
        StructField("acpn_3", IntegerType(), True),
        StructField("acpn_4", IntegerType(), True),
        StructField("acpn_5", IntegerType(), True),
        StructField("acpn_6", IntegerType(), True),
        StructField("acpn_7", IntegerType(), True),
        StructField("acpn_8", IntegerType(), True),
        StructField("acpn_9", IntegerType(), True),
        StructField("acpout_1", StringType(), True),
        StructField("acpout_2", StringType(), True),
        StructField("acpout_3", StringType(), True),
        StructField("acpout_4", StringType(), True),
        StructField("acpout_5", StringType(), True),
        StructField("acpout_6", StringType(), True),
        StructField("acpout_7", StringType(), True),
        StructField("acpout_8", StringType(), True),
        StructField("acpout_9", StringType(), True),
        StructField("acpsour_1", StringType(), True),
        StructField("acpsour_2", StringType(), True),
        StructField("acpsour_3", StringType(), True),
        StructField("acpsour_4", StringType(), True),
        StructField("acpsour_5", StringType(), True),
        StructField("acpsour_6", StringType(), True),
        StructField("acpsour_7", StringType(), True),
        StructField("acpsour_8", StringType(), True),
        StructField("acpsour_9", StringType(), True),
        StructField("acpspef_1", StringType(), True),
        StructField("acpspef_2", StringType(), True),
        StructField("acpspef_3", StringType(), True),
        StructField("acpspef_4", StringType(), True),
        StructField("acpspef_5", StringType(), True),
        StructField("acpspef_6", StringType(), True),
        StructField("acpspef_7", StringType(), True),
        StructField("acpspef_8", StringType(), True),
        StructField("acpspef_9", StringType(), True),
        StructField("acpstar_1", DateType(), True),
        StructField("acpstar_2", DateType(), True),
        StructField("acpstar_3", DateType(), True),
        StructField("acpstar_4", DateType(), True),
        StructField("acpstar_5", DateType(), True),
        StructField("acpstar_6", DateType(), True),
        StructField("acpstar_7", DateType(), True),
        StructField("acpstar_8", DateType(), True),
        StructField("acpstar_9", DateType(), True),
        StructField("depdays_1", IntegerType(), True),
        StructField("depdays_2", IntegerType(), True),
        StructField("depdays_3", IntegerType(), True),
        StructField("depdays_4", IntegerType(), True),
        StructField("depdays_5", IntegerType(), True),
        StructField("depdays_6", IntegerType(), True),
        StructField("depdays_7", IntegerType(), True),
        StructField("depdays_8", IntegerType(), True),
        StructField("depdays_9", IntegerType(), True),
        StructField("intdays_1", IntegerType(), True),
        StructField("intdays_2", IntegerType(), True),
        StructField("intdays_3", IntegerType(), True),
        StructField("intdays_4", IntegerType(), True),
        StructField("intdays_5", IntegerType(), True),
        StructField("intdays_6", IntegerType(), True),
        StructField("intdays_7", IntegerType(), True),
        StructField("intdays_8", IntegerType(), True),
        StructField("intdays_9", IntegerType(), True),
        StructField("numacp", IntegerType(), True),
        StructField("orgsup_1", IntegerType(), True),
        StructField("orgsup_2", IntegerType(), True),
        StructField("orgsup_3", IntegerType(), True),
        StructField("orgsup_4", IntegerType(), True),
        StructField("orgsup_5", IntegerType(), True),
        StructField("orgsup_6", IntegerType(), True),
        StructField("orgsup_7", IntegerType(), True),
        StructField("orgsup_8", IntegerType(), True),
        StructField("orgsup_9", IntegerType(), True),
        StructField("delprean", StringType(), True),
        StructField("delposan", StringType(), True),
        StructField("antedur", IntegerType(), True),
        StructField("birordr_1", StringType(), True),
        StructField("birordr_2", StringType(), True),
        StructField("birordr_3", StringType(), True),
        StructField("birordr_4", StringType(), True),
        StructField("birordr_5", StringType(), True),
        StructField("birordr_6", StringType(), True),
        StructField("birordr_7", StringType(), True),
        StructField("birordr_8", StringType(), True),
        StructField("birordr_9", StringType(), True),
        StructField("birweit_1", IntegerType(), True),
        StructField("birweit_2", IntegerType(), True),
        StructField("birweit_3", IntegerType(), True),
        StructField("birweit_4", IntegerType(), True),
        StructField("birweit_5", IntegerType(), True),
        StructField("birweit_6", IntegerType(), True),
        StructField("birweit_7", IntegerType(), True),
        StructField("birweit_8", IntegerType(), True),
        StructField("birweit_9", IntegerType(), True),
        StructField("delchang", StringType(), True),
        StructField("delmeth_1", StringType(), True),
        StructField("delmeth_2", StringType(), True),
        StructField("delmeth_3", StringType(), True),
        StructField("delmeth_4", StringType(), True),
        StructField("delmeth_5", StringType(), True),
        StructField("delmeth_6", StringType(), True),
        StructField("delmeth_7", StringType(), True),
        StructField("delmeth_8", StringType(), True),
        StructField("delmeth_9", StringType(), True),
        StructField("delplac_1", StringType(), True),
        StructField("delplac_2", StringType(), True),
        StructField("delplac_3", StringType(), True),
        StructField("delplac_4", StringType(), True),
        StructField("delplac_5", StringType(), True),
        StructField("delplac_6", StringType(), True),
        StructField("delplac_7", StringType(), True),
        StructField("delplac_8", StringType(), True),
        StructField("delplac_9", StringType(), True),
        StructField("delinten", StringType(), True),
        StructField("anasdate", DateType(), True),
        StructField("anagest", IntegerType(), True),
        StructField("gestat_1", IntegerType(), True),
        StructField("gestat_2", IntegerType(), True),
        StructField("gestat_3", IntegerType(), True),
        StructField("gestat_4", IntegerType(), True),
        StructField("gestat_5", IntegerType(), True),
        StructField("gestat_6", IntegerType(), True),
        StructField("gestat_7", IntegerType(), True),
        StructField("gestat_8", IntegerType(), True),
        StructField("gestat_9", IntegerType(), True),
        StructField("birstat_1", StringType(), True),
        StructField("birstat_2", StringType(), True),
        StructField("birstat_3", StringType(), True),
        StructField("birstat_4", StringType(), True),
        StructField("birstat_5", StringType(), True),
        StructField("birstat_6", StringType(), True),
        StructField("birstat_7", StringType(), True),
        StructField("birstat_8", StringType(), True),
        StructField("birstat_9", StringType(), True),
        StructField("delonset", IntegerType(), True),
        StructField("matage", IntegerType(), True),
        StructField("numbaby", StringType(), True),
        StructField("numpreg", IntegerType(), True),
        StructField("postdur", IntegerType(), True),
        StructField("biresus_1", StringType(), True),
        StructField("biresus_2", StringType(), True),
        StructField("biresus_3", StringType(), True),
        StructField("biresus_4", StringType(), True),
        StructField("biresus_5", StringType(), True),
        StructField("biresus_6", StringType(), True),
        StructField("biresus_7", StringType(), True),
        StructField("biresus_8", StringType(), True),
        StructField("biresus_9", StringType(), True),
        StructField("sexbaby_1", StringType(), True),
        StructField("sexbaby_2", StringType(), True),
        StructField("sexbaby_3", StringType(), True),
        StructField("sexbaby_4", StringType(), True),
        StructField("sexbaby_5", StringType(), True),
        StructField("sexbaby_6", StringType(), True),
        StructField("sexbaby_7", StringType(), True),
        StructField("sexbaby_8", StringType(), True),
        StructField("sexbaby_9", StringType(), True),
        StructField("delstat_1", StringType(), True),
        StructField("delstat_2", StringType(), True),
        StructField("delstat_3", StringType(), True),
        StructField("delstat_4", StringType(), True),
        StructField("delstat_5", StringType(), True),
        StructField("delstat_6", StringType(), True),
        StructField("delstat_7", StringType(), True),
        StructField("delstat_8", StringType(), True),
        StructField("delstat_9", StringType(), True),
        StructField("neocare", StringType(), True),
        StructField("well_baby_ind", StringType(), True),
        StructField("censage", IntegerType(), True),
        StructField("carersi", StringType(), True),
        StructField("detndate", DateType(), True),
        StructField("det_cfl", IntegerType(), True),
        StructField("cendur", IntegerType(), True),
        StructField("detdur", IntegerType(), True),
        StructField("marstat", StringType(), True),
        StructField("mentcat", StringType(), True),
        StructField("admistat", StringType(), True),
        StructField("censtat", StringType(), True),
        StructField("vind", StringType(), True),
        StructField("cenward", StringType(), True),
        StructField("subdate", DateType(), True),
        StructField("nhsnoind", StringType(), True),
        StructField("opcs43", StringType(), True),
        StructField("pcgorig", StringType(), True),
        StructField("pctorig", StringType(), True),
        StructField("pctorig06", StringType(), True),
        StructField("susrecid", StringType(), True),
        StructField("epikey", StringType(), True)
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

# some of the epikeys are broken and need to be fixed
if 1997 <= year < 2012:
    to_add = ((year % 100) + 100) * int(1e9)
    df = df.withColumn("epikey", (F.col("epikey").cast("long") + F.lit(to_add)).cast("string"))

mpsid = (
    spark.read.parquet(mpsid_file)
    .withColumnRenamed("tokenid", "person_id_deid")
)

df = df.join(mpsid, "epikey", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save
# MAGIC
# MAGIC Save the data to the data lake

# COMMAND ----------

(
    df.withColumn("fyear", F.lit(fyear))
    .select(*sorted(df.columns))
    .repartition(32)
    .write
    .option("mergeSchema", "true")
    .mode("append")
    .saveAsTable("hes.bronze.apc")
)
