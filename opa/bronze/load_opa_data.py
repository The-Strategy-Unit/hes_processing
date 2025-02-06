"""Load OPA Data"""

import sys

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark import SparkContext

from opa.bronze.get_opa_csv_data import get_opa_csv_data
from opa.bronze.get_opa_parquet_data import get_opa_parquet_data

TABLE = "hes.bronze.opa"


def load_opa_data(spark: SparkContext, year: int) -> None:
    """Load OPA data"""
    fyear = year * 100 + ((year + 1) % 100)
    fn = get_opa_parquet_data if year >= 2021 else get_opa_csv_data

    df = fn(spark, year)

    # create the table if it does not exist
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(TABLE)
        .addColumns(df.schema)
        .execute()
    )

    # delete rows of data if they already existed
    spark.sql(f"DELETE FROM {TABLE} WHERE fyear = {fyear}")

    # save data
    (
        df.select(*sorted(df.columns))
        .repartition(32)
        .write.option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    load_opa_data(DatabricksSession.builder.getOrCreate(), int(sys.argv[1]))
