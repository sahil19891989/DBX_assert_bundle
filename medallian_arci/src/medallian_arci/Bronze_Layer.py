"""Bronze layer ingest for the lapse pipeline.

Reads raw policies.csv from the Unity Catalog volume and writes the
bronze_policies Delta table with ingestion metadata columns.
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze layer ingest for the lapse pipeline.")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog (e.g. metlife_dmo)")
    parser.add_argument("--schema", required=True, help="Schema for the pipeline tables (e.g. lapse)")
    args = parser.parse_args()

    SOURCE_PATH = f"/Volumes/{args.catalog}/{args.schema}/raw_files/policies.csv"
    TARGET_TABLE = f"{args.catalog}.{args.schema}.bronze_policies"

    spark = SparkSession.builder.getOrCreate()

    bronze = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(SOURCE_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())          # when we loaded it
        .withColumn("_source_file", F.col("_metadata.file_path"))  # where it came from
    )

    bronze.write.mode("overwrite").saveAsTable(TARGET_TABLE)

    print(f"Wrote {bronze.count()} rows to {TARGET_TABLE}")


if __name__ == "__main__":
    main()
