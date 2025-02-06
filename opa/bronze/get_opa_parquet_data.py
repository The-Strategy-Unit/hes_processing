"""Get OPA (parquet) data"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def get_opa_parquet_data(spark: SparkContext, year: int) -> DataFrame:
    """Get OPA (parquet) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = "abfss://nhse-nhp-data@sudata.dfs.core.windows.net/NHP_HES/NHP_HESOPA/"

    df = spark.read.parquet(filepath).filter(F.col("fyear") == (fyear % 10000))

    # convert all column names to lower case
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

    df = (
        df.withColumnRenamed("apptdate_derived", "apptdate")
        .withColumnRenamed("dnadate_derived", "dnadate")
        .withColumnRenamed("rttperend", "rttperend")
        .withColumnRenamed("rttperstart_derived", "rttperstart")
        .withColumnRenamed("token_person_id", "person_id_deid")
        .drop("fileid", "period", "subperiod", "sourcesystem", "fyear")
    )

    # convert some columns
    df = reduce(
        lambda x, y: x.withColumn(y, F.col(y).cast(T.IntegerType())),
        ["apptage", "partyear"],
        df,
    )

    df = reduce(
        lambda x, y: x.withColumn(y, F.col(y).cast(T.FloatType())),
        [
            "imd04",
            "imd04c",
            "imd04ed",
            "imd04em",
            "imd04hd",
            "imd04hs",
            "imd04i",
            "imd04ia",
            "imd04ic",
            "imd04le",
            "imd04rk",
        ],
        df,
    )
    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    return df
