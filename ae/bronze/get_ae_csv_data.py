"""Get A&E (CSV) data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ae.bronze.schemas import create_schema


def get_ae_csv_data(spark: SparkContext, year: int) -> DataFrame:
    """Get A&E (CSV) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = f"/Volumes/hes/bronze/files/ae/ae_{fyear}"

    sep = "|" if year < 2019 else ","
    csv_schema = create_schema(f"{filepath}/ae_{fyear}_headers.txt", sep)

    df: DataFrame = spark.read.csv(
        f"{filepath}/data", header=False, schema=csv_schema, sep=sep
    )

    # join in mpsid file
    if year <= 2018:
        mpsid_file = f"{filepath}/ae_{fyear}_mpsid.parquet"

        if year < 2012:
            to_add = (200 + (year % 100)) * int(1e9)
            df = df.withColumn(
                "aekey", (F.col("aekey").cast("long") + F.lit(to_add)).cast("string")
            )

        mpsid = spark.read.parquet(mpsid_file).withColumnRenamed(
            "tokenid", "person_id_deid"
        )

        df = df.join(mpsid, "aekey", "left")

    # recalculate resladst_ons/resgor_ons
    if 2009 <= year <= 2014:
        resladst_lkup = (
            spark.read.table("strategyunit.reference.resladst_to_resladst_ons_lookup")
            .filter(F.col("year") == year)
            .drop("year")
        )
        df = df.join(resladst_lkup, "resladst", "left")

        resgor_lkup = spark.read.table(
            "strategyunit.reference.resgor_to_resgor_ons_lookup"
        )
        df = df.join(resgor_lkup, "resgor", "left")

    # remove the soal/soam columns if they haven't been remaped to lsoa01/msoa01
    if "soal" in df.columns:
        df = df.drop("soal", "soam")

    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    return df
