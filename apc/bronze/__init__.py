from functools import reduce
from pyspark import SparkContext
from pyspark.sql import DataFrame, functions as F, types as T
from databricks.connect import DatabricksSession
import sys

from apc.bronze.schemas import columns

def _load_apc_csv_data(spark: SparkContext, year: int) -> None:
    fyear = year * 100 + ((year + 1) % 100)

    filepath = "/Volumes/su_data/default/hes_raw/apc/"
    filename = f"{filepath}/apc_{fyear}"

    csv_schema = columns[year]
    
    df: DataFrame = (
        spark.read
        .csv(
            filename,
            header=False,
            schema=csv_schema,
            sep="|" if year < 2019 else ","
        )
    )

    # join in mpsid file
    if 1997 <= year <= 2018:
        mpsid_file = f"{filepath}/apc_{fyear}_mpsid.parquet"

        if 1997 <= year < 2012:
            to_add = ((year % 100) + 100) * int(1e9)
            df = df.withColumn("epikey", (F.col("epikey").cast("long") + F.lit(to_add)).cast("string"))

        mpsid = (
            spark.read.parquet(mpsid_file)
            .withColumnRenamed("tokenid", "person_id_deid")
        )

        df = df.join(mpsid, "epikey", "left")

    # load missing dismeth
    if 2019 <= year <= 2020:
        dismeth_file = f"{filepath}/apc_{fyear}_dismeth.csv.gz"

        dismeth = (
            spark.read
            .csv(
                dismeth_file,
                header=True,
                sep=","
            )
            .select("epikey", "dismeth")
        )

        df = df.join(dismeth, "epikey", "left")
    
    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    # save data
    (
        df
        .select(*sorted(df.columns))
        .repartition(32)
        .write
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("hes.bronze.apc")
    )

def _load_apc_parquet_data(spark: SparkContext, year: int) -> None:
    fyear = year * 100 + ((year + 1) % 100)

    filepath = "/Volumes/su_data/default/nhp_hes_apc/"

    if year >= 2023:
        filepath += "/FY2023-24/"

    df = (
        spark.read
        .parquet(filepath)
        .filter(F.col("fyear") == (fyear % 10000))
    )

    # convert all column names to lower case
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

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
    
    # convert some columns
    df = reduce(
        lambda x, y: x.withColumn(y, F.col(y).cast(T.IntegerType())),
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

    df = reduce(
        lambda x, y: x.withColumn(y, F.col(y).cast(T.FloatType())),
        ["imd04", "imd04c", "imd04ed", "imd04em", "imd04hd", "imd04hs", "imd04i", "imd04ia", "imd04ic", "imd04le", "imd04rk"],
        df
    )

    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    (
        df
        .select(*sorted(df.columns))
        .repartition(32)
        .write
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("hes.bronze.apc")
    )

def load_apc_data(spark: SparkContext, year: int) -> None:
    if year >= 2021:
        _load_apc_parquet_data(spark, year)
    else:
        _load_apc_csv_data(spark, year)

if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    year = sys.argv[1]
    load_apc_data(spark, year)