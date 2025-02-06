"""Load A&E Data"""

import sys

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark import SparkContext

from ae.bronze.get_ae_csv_data import get_ae_csv_data

TABLE = "hes.bronze.aae"


def load_aae_data(spark: SparkContext, year: int) -> None:
    """Load A&E data"""
    fyear = year * 100 + ((year + 1) % 100)

    df = get_ae_csv_data(spark, year)

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
    load_aae_data(DatabricksSession.builder.getOrCreate(), int(sys.argv[1]))
