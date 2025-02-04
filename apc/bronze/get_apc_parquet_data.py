from functools import reduce
from pyspark import SparkContext
from pyspark.sql import functions as F, types as T, DataFrame

def get_apc_parquet_data(spark: SparkContext, year: int) -> DataFrame:
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

    return df