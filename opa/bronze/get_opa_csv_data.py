"""Get OPA (CSV) data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opa.bronze.schemas import create_schema


def get_opa_csv_data(spark: SparkContext, year: int) -> DataFrame:
    """Get OPA (CSV) data"""
    fyear = year * 100 + ((year + 1) % 100)

    filepath = f"/Volumes/su_data/default/hes_raw/opa/opa_{fyear}"

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
        df = df.withColumnRenamed("1stoperation", "opertn_01")

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

    # add fyear
    df = df.withColumn("fyear", F.lit(fyear))

    return df
