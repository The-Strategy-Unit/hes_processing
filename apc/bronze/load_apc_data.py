from pyspark import SparkContext
from databricks.connect import DatabricksSession
import sys

from apc.bronze.schemas import columns

from apc.bronze.get_apc_csv_data import get_apc_csv_data
from apc.bronze.get_apc_parquet_data import get_apc_parquet_data

def load_apc_data(spark: SparkContext, year: int) -> None:
    if year >= 2021:
        df = get_apc_parquet_data(spark, year)
    else:
        df = get_apc_csv_data(spark, year)
    
    # save data
    (
        df
        .select(*sorted(df.columns))
        .repartition(32)
        .write
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("hes.bronze.apc")
    )



if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    year = int(sys.argv[1])
    load_apc_data(spark, year)