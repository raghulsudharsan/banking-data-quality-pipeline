import os

from pyspark.sql import SparkSession

BUCKET = "banking-data-quality-pipeline-975050313754-ap-south-1-an"

application_path = "hdfs://localhost:9000/banking/processed/application"
raw_bureau_path = f"s3a://{BUCKET}/input/bureau.csv"
valid_bureau_path = "hdfs://localhost:9000/banking/processed/bureau_valid"
error_path = "hdfs://localhost:9000/banking/errors/orphan_bureau_records"

spark = (
    SparkSession.builder
    .appName("QuarantineOrphanBureauRecords")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

try:
    application_df = spark.read.parquet(application_path)

    bureau_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_bureau_path)
    )

    application_ids = application_df.select("SK_ID_CURR").distinct()

    orphan_bureau = bureau_df.join(
        application_ids, on="SK_ID_CURR", how="left_anti"
    )

    valid_bureau = bureau_df.join(
        application_ids, on="SK_ID_CURR", how="left_semi"
    )

    total_records = bureau_df.count()
    valid_records = valid_bureau.count()
    orphan_records = orphan_bureau.count()

    print("\nBureau Quarantine Summary")
    print("-------------------------")
    print(f"Total bureau records : {total_records}")
    print(f"Valid bureau records : {valid_records}")
    print(f"Orphan bureau records: {orphan_records}")

    if orphan_records > 0:
        orphan_bureau.write.mode("overwrite").parquet(error_path)

    valid_bureau.write.mode("overwrite").parquet(valid_bureau_path)

    print(f"Valid bureau output: {valid_bureau_path}")
    print(f"Orphan records output: {error_path}")

finally:
    spark.stop()
