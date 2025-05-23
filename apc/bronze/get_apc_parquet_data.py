"""Get APC (parquet) data"""

from functools import reduce

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def get_apc_parquet_data(spark: SparkContext, year: int) -> DataFrame:
    """Get APC (parquet) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = "abfss://nhse-nhp-data@sudata.dfs.core.windows.net/NHP_HES/NHP_HESAPC/"

    if year >= 2023:
        filepath += "/FY2023-24/"

    df = spark.read.parquet(filepath).filter(F.col("fyear") == (fyear % 10000))

    # convert all column names to lower case
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])

    df = (
        df.withColumnRenamed("acscflag_derived", "acscflag")
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
            "spelbgin",
        ],
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

    # add maternity_episode_type
    df_mat_episode_type = (
        df.filter(F.col("epistat") == "3")
        .filter(F.col("classpat").isin(1, 2, 5))
        .filter(F.col("delmeth_1").isNotNull())
        .withColumn(
            "maternity_episode_type",
            F.when(
                F.col("epitype").isin(2, 5),
                F.when(F.col("delplac_1").isin(1, 5, 6), 3).otherwise(1),
            )
            .when(
                F.col("epitype").isin(3, 6),
                F.when(F.col("delplac_1").isin(1, 5, 6), 4).otherwise(2),
            )
            .otherwise("9"),
        )
        .select("epikey", "maternity_episode_type")
    )

    df = df.join(df_mat_episode_type, "epikey", "left")

    return df
