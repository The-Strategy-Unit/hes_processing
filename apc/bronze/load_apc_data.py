"""Load APC Data"""

import sys

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark import SparkContext

from apc.bronze.get_apc_csv_data import get_apc_csv_data
from apc.bronze.get_apc_parquet_data import get_apc_parquet_data

TABLE = "hes.bronze.apc"


def load_apc_data(spark: SparkContext, year: int) -> None:
    """Load APC data"""
    fyear = year * 100 + ((year + 1) % 100)
    fn = get_apc_parquet_data if year >= 2021 else get_apc_csv_data

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
    load_apc_data(DatabricksSession.builder.getOrCreate(), int(sys.argv[1]))
