"""Get APC (CSV) data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from apc.bronze.schemas import create_schema


def get_apc_csv_data(spark: SparkContext, year: int) -> DataFrame:
    """Get APC (CSV) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = f"/Volumes/su_data/default/hes_raw/apc/apc_{fyear}"

    sep = "|" if year < 2019 else ","
    csv_schema = create_schema(f"{filepath}/apc_{fyear}_headers.txt", sep)

    df: DataFrame = spark.read.csv(
        f"{filepath}/data", header=False, schema=csv_schema, sep=sep
    )

    # join in mpsid file
    if 1997 <= year <= 2018:
        mpsid_file = f"{filepath}/apc_{fyear}_mpsid.parquet"

        if year < 2012:
            to_add = (100 + (year % 100)) * int(1e9)
            df = df.withColumn(
                "epikey", (F.col("epikey").cast("long") + F.lit(to_add)).cast("string")
            )

        mpsid = spark.read.parquet(mpsid_file).withColumnRenamed(
            "tokenid", "person_id_deid"
        )

        df = df.join(mpsid, "epikey", "left")

    # load missing dismeth
    if 2019 <= year <= 2020:
        dismeth_file = f"{filepath}/apc_{fyear}_dismeth.csv.gz"

        dismeth = spark.read.csv(dismeth_file, header=True, sep=",").select(
            "epikey", "dismeth"
        )

        df = df.join(dismeth, "epikey", "left")

    # remove the soal/soam columns if they haven't been remaped to lsoa01/msoa01
    if "soal" in df.columns:
        df = df.drop("soal", "soam")

    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    return df
