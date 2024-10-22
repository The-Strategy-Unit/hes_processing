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
# MAGIC ## Changes for different years
# MAGIC
# MAGIC ### 2019/20 and 2020/21
# MAGIC
# MAGIC In these years, the provided files missed out the `DISMETH` field initially. This field was provided as a supplementary file which has to be joined.

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

savepath = f"/Volumes/hes/bronze/raw/apc/fyear={fyear}"

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

csv_schema = StructType([
    StructField('acscflag', IntegerType(), True),
    StructField('activage', IntegerType(), True),
    StructField('admiage', IntegerType(), True),
    StructField('admidate', DateType(), True),
    StructField('admimeth', StringType(), True),
    StructField('admincat', StringType(), True),
    StructField('admincatst', StringType(), True),
    StructField('admisorc', StringType(), True),
    StructField('admistat', StringType(), True),
    StructField('aekey', StringType(), True),
    StructField('alcdiag', StringType(), True),
    StructField('alcdiag_4', StringType(), True),
    StructField('alcfrac', FloatType(), True),
    StructField('anagest', IntegerType(), True),
    StructField('anasdate', DateType(), True),
    StructField('antedur', IntegerType(), True),
    StructField('at_gp_practice', StringType(), True),
    StructField('at_residence', StringType(), True),
    StructField('at_treatment', StringType(), True),
    StructField('bedyear', IntegerType(), True),
    StructField('biresus_1', StringType(), True),
    StructField('biresus_2', StringType(), True),
    StructField('biresus_3', StringType(), True),
    StructField('biresus_4', StringType(), True),
    StructField('biresus_5', StringType(), True),
    StructField('biresus_6', StringType(), True),
    StructField('biresus_7', StringType(), True),
    StructField('biresus_8', StringType(), True),
    StructField('biresus_9', StringType(), True),
    StructField('birordr_1', StringType(), True),
    StructField('birordr_2', StringType(), True),
    StructField('birordr_3', StringType(), True),
    StructField('birordr_4', StringType(), True),
    StructField('birordr_5', StringType(), True),
    StructField('birordr_6', StringType(), True),
    StructField('birordr_7', StringType(), True),
    StructField('birordr_8', StringType(), True),
    StructField('birordr_9', StringType(), True),
    StructField('birstat_1', StringType(), True),
    StructField('birstat_2', StringType(), True),
    StructField('birstat_3', StringType(), True),
    StructField('birstat_4', StringType(), True),
    StructField('birstat_5', StringType(), True),
    StructField('birstat_6', StringType(), True),
    StructField('birstat_7', StringType(), True),
    StructField('birstat_8', StringType(), True),
    StructField('birstat_9', StringType(), True),
    StructField('birweit_1', IntegerType(), True),
    StructField('birweit_2', IntegerType(), True),
    StructField('birweit_3', IntegerType(), True),
    StructField('birweit_4', IntegerType(), True),
    StructField('birweit_5', IntegerType(), True),
    StructField('birweit_6', IntegerType(), True),
    StructField('birweit_7', IntegerType(), True),
    StructField('birweit_8', IntegerType(), True),
    StructField('birweit_9', IntegerType(), True),
    StructField('cannet', StringType(), True),
    StructField('canreg', StringType(), True),
    StructField('carersi', StringType(), True),
    StructField('cause', StringType(), True),
    StructField('cause_3', StringType(), True),
    StructField('cause_4', StringType(), True),
    StructField('ccg_gp_practice', StringType(), True),
    StructField('ccg_residence', StringType(), True),
    StructField('ccg_responsibility', StringType(), True),
    StructField('ccg_responsibility_origin', StringType(), True),
    StructField('ccg_treatment', StringType(), True),
    StructField('ccg_treatment_origin', StringType(), True),
    StructField('cdsextdate', DateType(), True),
    StructField('cdsverprotid', StringType(), True),
    StructField('cdsversion', StringType(), True),
    StructField('cendur', IntegerType(), True),
    StructField('censage', IntegerType(), True),
    StructField('censtat', StringType(), True),
    StructField('cenward', StringType(), True),
    StructField('classpat', StringType(), True),
    StructField('cr_gp_practice', StringType(), True),
    StructField('cr_residence', StringType(), True),
    StructField('cr_treatment', StringType(), True),
    StructField('csnum', StringType(), True),
    StructField('currward', StringType(), True),
    StructField('currward_ons', StringType(), True),
    StructField('delchang', StringType(), True),
    StructField('delinten', StringType(), True),
    StructField('delmeth_1', StringType(), True),
    StructField('delmeth_2', StringType(), True),
    StructField('delmeth_3', StringType(), True),
    StructField('delmeth_4', StringType(), True),
    StructField('delmeth_5', StringType(), True),
    StructField('delmeth_6', StringType(), True),
    StructField('delmeth_7', StringType(), True),
    StructField('delmeth_8', StringType(), True),
    StructField('delmeth_9', StringType(), True),
    StructField('delmeth_d', StringType(), True),
    StructField('delonset', IntegerType(), True),
    StructField('delplac_1', StringType(), True),
    StructField('delplac_2', StringType(), True),
    StructField('delplac_3', StringType(), True),
    StructField('delplac_4', StringType(), True),
    StructField('delplac_5', StringType(), True),
    StructField('delplac_6', StringType(), True),
    StructField('delplac_7', StringType(), True),
    StructField('delplac_8', StringType(), True),
    StructField('delplac_9', StringType(), True),
    StructField('delposan', StringType(), True),
    StructField('delprean', StringType(), True),
    StructField('delstat_1', StringType(), True),
    StructField('delstat_2', StringType(), True),
    StructField('delstat_3', StringType(), True),
    StructField('delstat_4', StringType(), True),
    StructField('delstat_5', StringType(), True),
    StructField('delstat_6', StringType(), True),
    StructField('delstat_7', StringType(), True),
    StructField('delstat_8', StringType(), True),
    StructField('delstat_9', StringType(), True),
    StructField('detdur', IntegerType(), True),
    StructField('detndate', DateType(), True),
    StructField('diag_01', StringType(), True),
    StructField('diag_02', StringType(), True),
    StructField('diag_03', StringType(), True),
    StructField('diag_04', StringType(), True),
    StructField('diag_05', StringType(), True),
    StructField('diag_06', StringType(), True),
    StructField('diag_07', StringType(), True),
    StructField('diag_08', StringType(), True),
    StructField('diag_09', StringType(), True),
    StructField('diag_10', StringType(), True),
    StructField('diag_11', StringType(), True),
    StructField('diag_12', StringType(), True),
    StructField('diag_13', StringType(), True),
    StructField('diag_14', StringType(), True),
    StructField('diag_15', StringType(), True),
    StructField('diag_16', StringType(), True),
    StructField('diag_17', StringType(), True),
    StructField('diag_18', StringType(), True),
    StructField('diag_19', StringType(), True),
    StructField('diag_20', StringType(), True),
    StructField('diag_count', IntegerType(), True),
    StructField('disdate', DateType(), True),
    StructField('disdest', StringType(), True),
    StructField('dismeth', StringType(), True),
    StructField('disreadydate', DateType(), True),
    StructField('earldatoff', DateType(), True),
    StructField('elecdate', DateType(), True),
    StructField('elecdur', IntegerType(), True),
    StructField('elecdur_calc', IntegerType(), True),
    StructField('encrypted_hesid', StringType(), True),
    StructField('endage', IntegerType(), True),
    StructField('epidur', IntegerType(), True),
    StructField('epiend', DateType(), True),
    StructField('epikey', StringType(), True),
    StructField('epiorder', IntegerType(), True),
    StructField('epistart', DateType(), True),
    StructField('epistat', IntegerType(), True),
    StructField('epitype', IntegerType(), True),
    StructField('ethnos', StringType(), True),
    StructField('ethraw', StringType(), True),
    StructField('fae', IntegerType(), True),
    StructField('fae_emergency', IntegerType(), True),
    StructField('fce', IntegerType(), True),
    StructField('fde', IntegerType(), True),
    StructField('firstreg', StringType(), True),
    StructField('fyear', StringType(), True),
    StructField('gestat_1', IntegerType(), True),
    StructField('gestat_2', IntegerType(), True),
    StructField('gestat_3', IntegerType(), True),
    StructField('gestat_4', IntegerType(), True),
    StructField('gestat_5', IntegerType(), True),
    StructField('gestat_6', IntegerType(), True),
    StructField('gestat_7', IntegerType(), True),
    StructField('gestat_8', IntegerType(), True),
    StructField('gestat_9', IntegerType(), True),
    StructField('gortreat', StringType(), True),
    StructField('gpprac', StringType(), True),
    StructField('gppracha', StringType(), True),
    StructField('gppracro', StringType(), True),
    StructField('gpprpct', StringType(), True),
    StructField('gpprstha', StringType(), True),
    StructField('hatreat', StringType(), True),
    StructField('hneoind', StringType(), True),
    StructField('imd04', FloatType(), True),
    StructField('imd04_decile', StringType(), True),
    StructField('imd04c', FloatType(), True),
    StructField('imd04ed', FloatType(), True),
    StructField('imd04em', FloatType(), True),
    StructField('imd04hd', FloatType(), True),
    StructField('imd04hs', FloatType(), True),
    StructField('imd04i', FloatType(), True),
    StructField('imd04ia', FloatType(), True),
    StructField('imd04ic', FloatType(), True),
    StructField('imd04le', FloatType(), True),
    StructField('imd04rk', FloatType(), True),
    StructField('intmanig', StringType(), True),
    StructField('lsoa01', StringType(), True),
    StructField('lsoa11', StringType(), True),
    StructField('mainspef', StringType(), True),
    StructField('marstat', StringType(), True),
    StructField('matage', IntegerType(), True),
    StructField('maternity_episode_type', StringType(), True),
    StructField('mentcat', StringType(), True),
    StructField('msoa01', StringType(), True),
    StructField('msoa11', StringType(), True),
    StructField('mydob', StringType(), True),
    StructField('neocare', StringType(), True),
    StructField('neodur', IntegerType(), True),
    StructField('newnhsno_check', StringType(), True),
    StructField('nhsnoind', StringType(), True),
    StructField('numbaby', StringType(), True),
    StructField('numpreg', IntegerType(), True),
    StructField('numtailb', IntegerType(), True),
    StructField('oacode6', StringType(), True),
    StructField('opcs43', StringType(), True),
    StructField('opdate_01', DateType(), True),
    StructField('opdate_02', DateType(), True),
    StructField('opdate_03', DateType(), True),
    StructField('opdate_04', DateType(), True),
    StructField('opdate_05', DateType(), True),
    StructField('opdate_06', DateType(), True),
    StructField('opdate_07', DateType(), True),
    StructField('opdate_08', DateType(), True),
    StructField('opdate_09', DateType(), True),
    StructField('opdate_10', DateType(), True),
    StructField('opdate_11', DateType(), True),
    StructField('opdate_12', DateType(), True),
    StructField('opdate_13', DateType(), True),
    StructField('opdate_14', DateType(), True),
    StructField('opdate_15', DateType(), True),
    StructField('opdate_16', DateType(), True),
    StructField('opdate_17', DateType(), True),
    StructField('opdate_18', DateType(), True),
    StructField('opdate_19', DateType(), True),
    StructField('opdate_20', DateType(), True),
    StructField('opdate_21', DateType(), True),
    StructField('opdate_22', DateType(), True),
    StructField('opdate_23', DateType(), True),
    StructField('opdate_24', DateType(), True),
    StructField('operstat', StringType(), True),
    StructField('opertn_01', StringType(), True),
    StructField('opertn_02', StringType(), True),
    StructField('opertn_03', StringType(), True),
    StructField('opertn_04', StringType(), True),
    StructField('opertn_05', StringType(), True),
    StructField('opertn_06', StringType(), True),
    StructField('opertn_07', StringType(), True),
    StructField('opertn_08', StringType(), True),
    StructField('opertn_09', StringType(), True),
    StructField('opertn_10', StringType(), True),
    StructField('opertn_11', StringType(), True),
    StructField('opertn_12', StringType(), True),
    StructField('opertn_13', StringType(), True),
    StructField('opertn_14', StringType(), True),
    StructField('opertn_15', StringType(), True),
    StructField('opertn_16', StringType(), True),
    StructField('opertn_17', StringType(), True),
    StructField('opertn_18', StringType(), True),
    StructField('opertn_19', StringType(), True),
    StructField('opertn_20', StringType(), True),
    StructField('opertn_21', StringType(), True),
    StructField('opertn_22', StringType(), True),
    StructField('opertn_23', StringType(), True),
    StructField('opertn_24', StringType(), True),
    StructField('opertn_count', IntegerType(), True),
    StructField('orgpppid', StringType(), True),
    StructField('partyear', IntegerType(), True),
    StructField('pcfound', StringType(), True),
    StructField('pcon', StringType(), True),
    StructField('pcon_ons', StringType(), True),
    StructField('pconsult', StringType(), True),
    StructField('pctcode06', StringType(), True),
    StructField('pctnhs', StringType(), True),
    StructField('pctorig06', StringType(), True),
    StructField('pcttreat', StringType(), True),
    StructField('posopdur', IntegerType(), True),
    StructField('postdist', StringType(), True),
    StructField('postdur', IntegerType(), True),
    StructField('preferer', StringType(), True),
    StructField('preggmp', StringType(), True),
    StructField('preopdur', IntegerType(), True),
    StructField('primercp', StringType(), True),
    StructField('procode', StringType(), True),
    StructField('procode3', StringType(), True),
    StructField('procode5', StringType(), True),
    StructField('procodet', StringType(), True),
    StructField('protype', StringType(), True),
    StructField('provspnops', StringType(), True),
    StructField('purcode', StringType(), True),
    StructField('purval', StringType(), True),
    StructField('referorg', StringType(), True),
    StructField('rescty', StringType(), True),
    StructField('rescty_ons', StringType(), True),
    StructField('resgor', StringType(), True),
    StructField('resgor_ons', StringType(), True),
    StructField('resha', StringType(), True),
    StructField('resladst', StringType(), True),
    StructField('resladst_currward', StringType(), True),
    StructField('resladst_ons', StringType(), True),
    StructField('respct_his', StringType(), True),
    StructField('respct06', StringType(), True),
    StructField('resro', StringType(), True),
    StructField('resstha_his', StringType(), True),
    StructField('resstha06', StringType(), True),
    StructField('rotreat', StringType(), True),
    StructField('rttperend', DateType(), True),
    StructField('rttperstart', DateType(), True),
    StructField('rttperstat', StringType(), True),
    StructField('rururb_ind', StringType(), True),
    StructField('sender', StringType(), True),
    StructField('sex', StringType(), True),
    StructField('sexbaby_1', StringType(), True),
    StructField('sexbaby_2', StringType(), True),
    StructField('sexbaby_3', StringType(), True),
    StructField('sexbaby_4', StringType(), True),
    StructField('sexbaby_5', StringType(), True),
    StructField('sexbaby_6', StringType(), True),
    StructField('sexbaby_7', StringType(), True),
    StructField('sexbaby_8', StringType(), True),
    StructField('sexbaby_9', StringType(), True),
    StructField('sitetret', StringType(), True),
    StructField('spelbgin', IntegerType(), True),
    StructField('speldur', IntegerType(), True),
    StructField('spelend', StringType(), True),
    StructField('startage', IntegerType(), True),
    StructField('startage_calc', FloatType(), True),
    StructField('sthatret', StringType(), True),
    StructField('subdate', DateType(), True),
    StructField('suscorehrg', StringType(), True),
    StructField('sushrg', StringType(), True),
    StructField('sushrgvers', StringType(), True),
    StructField('suslddate', DateType(), True),
    StructField('susrecid', StringType(), True),
    StructField('susspellid', StringType(), True),
    StructField('tretspef', StringType(), True),
    StructField('waitdays', IntegerType(), True),
    StructField('waitlist', IntegerType(), True),
    StructField('well_baby_ind', StringType(), True)
])


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

df = df.join(mpsid, "epikey", "left")

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
