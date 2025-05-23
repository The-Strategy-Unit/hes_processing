"""Get OPA (CSV) data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opa.bronze.schemas import create_schema


def get_opa_csv_data(spark: SparkContext, year: int) -> DataFrame:
    """Get OPA (CSV) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = f"/Volumes/hes/bronze/files/opa/opa_{fyear}"

    sep = "|" if year < 2019 else ","
    csv_schema = create_schema(f"{filepath}/opa_{fyear}_headers.txt", sep)

    df: DataFrame = spark.read.csv(
        f"{filepath}/data", header=False, schema=csv_schema, sep=sep
    )

    # join in mpsid file
    if year <= 2018:
        mpsid_file = f"{filepath}/opa_{fyear}_mpsid.parquet"

        if year < 2012:
            to_add = (300 + (year % 100)) * int(1e9)
            df = df.withColumn(
                "attendkey",
                (F.col("attendkey").cast("long") + F.lit(to_add)).cast("string"),
            )

        mpsid = spark.read.parquet(mpsid_file).withColumnRenamed(
            "tokenid", "person_id_deid"
        )

        df = df.join(mpsid, "attendkey", "left")

    if "1stoperation" in df.columns:
        if year >= 2019:
            df = df.withColumnRenamed("1stoperation", "opertn_01")
        else:
            df = df.drop("1stoperation")

    if year >= 2019:
        df = df.drop(
            *[
                i
                for i in df.columns
                if i.startswith("diag_3_")
                or i.startswith("diag_4_")
                or i.startswith("opertn_3_")
                or i.startswith("opertn_4_")
                or i.endswith("count")
            ]
        )

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
