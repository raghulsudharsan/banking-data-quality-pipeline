import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

BUCKET = "banking-data-quality-pipeline-975050313754-ap-south-1-an"

input_path = os.getenv(
    "APPLICATION_INPUT_PATH",
    f"s3a://{BUCKET}/input/application_train.csv",
)
output_path = os.getenv(
    "APPLICATION_OUTPUT_PATH",
    "hdfs://localhost:9000/banking/processed/application",
)

spark = (
    SparkSession.builder
    .appName("BankingApplicationTransformation")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-south-1")
    .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

try:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    application_df = df.select(
        "SK_ID_CURR",
        "TARGET",
        "NAME_CONTRACT_TYPE",
        "CODE_GENDER",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
        "NAME_INCOME_TYPE",
        "NAME_EDUCATION_TYPE",
        "NAME_FAMILY_STATUS",
        "CNT_CHILDREN",
        "CNT_FAM_MEMBERS",
        "EXT_SOURCE_2",
        "EXT_SOURCE_3",
        "DAYS_BIRTH",
    )

    application_df = (
        application_df
        .withColumn(
            "AGE_YEARS",
            round(-col("DAYS_BIRTH") / 365.25, 1),
        )
        .withColumn(
            "CREDIT_INCOME_RATIO",
            round(col("AMT_CREDIT") / col("AMT_INCOME_TOTAL"), 2),
        )
        .withColumn(
            "ANNUITY_INCOME_RATIO",
            round(col("AMT_ANNUITY") / col("AMT_INCOME_TOTAL"), 2),
        )
        .drop("DAYS_BIRTH")
    )

    application_df.write.mode("overwrite").parquet(output_path)

    print(f"Application transformation completed: {input_path}")
    print(f"HDFS Parquet output: {output_path}")

finally:
    spark.stop()
