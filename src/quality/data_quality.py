from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("BankingDataQuality")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

try:
    application_df = spark.read.parquet(
        "hdfs://localhost:9000/banking/processed/application"
    )

    bureau_valid_df = spark.read.parquet(
        "hdfs://localhost:9000/banking/processed/bureau_valid"
    )

    customer_df = spark.read.parquet(
        "hdfs://localhost:9000/banking/processed/customer"
    )

    def check(condition, name):
        if condition:
            print(f"PASS: {name}")
        else:
            print(f"FAIL: {name}")
            raise Exception(f"Data Quality Check Failed: {name}")

    print("\n========== DATA QUALITY REPORT ==========\n")

    # 1. Application customer ID not null
    null_application_ids = (
        application_df.filter(col("SK_ID_CURR").isNull()).count()
    )
    check(
        null_application_ids == 0,
        "Application SK_ID_CURR is not null",
    )

    # 2. Application customer ID uniqueness
    application_records = application_df.count()
    application_distinct_ids = (
        application_df.select("SK_ID_CURR").distinct().count()
    )
    check(
        application_records == application_distinct_ids,
        "Application SK_ID_CURR is unique",
    )

    # 3. Valid bureau ID uniqueness
    bureau_records = bureau_valid_df.count()
    bureau_distinct_ids = (
        bureau_valid_df.select("SK_ID_BUREAU").distinct().count()
    )
    check(
        bureau_records == bureau_distinct_ids,
        "Valid bureau SK_ID_BUREAU is unique",
    )

    # 4. Income validation: reject null and non-positive values
    invalid_income = (
        application_df
        .filter(
            col("AMT_INCOME_TOTAL").isNull()
            | (col("AMT_INCOME_TOTAL") <= 0)
        )
        .count()
    )
    check(
        invalid_income == 0,
        "AMT_INCOME_TOTAL is present and positive",
    )

    # 5. Credit validation: reject null and non-positive values
    invalid_credit = (
        application_df
        .filter(
            col("AMT_CREDIT").isNull()
            | (col("AMT_CREDIT") <= 0)
        )
        .count()
    )
    check(
        invalid_credit == 0,
        "AMT_CREDIT is present and positive",
    )

    # 6. Customer dataset key validation
    null_customer_ids = (
        customer_df.filter(col("SK_ID_CURR").isNull()).count()
    )
    check(
        null_customer_ids == 0,
        "Customer SK_ID_CURR is not null",
    )

    # 7. Customer count must match application count
    customer_records = customer_df.count()
    check(
        customer_records == application_records,
        "Customer record count matches application record count",
    )

    print("\n========== QUARANTINE INFORMATION ==========\n")
    print("Orphan bureau records are excluded from downstream processing.")
    print(f"Valid bureau records : {bureau_records}")
    print(f"Customer records     : {customer_records}")

    print("\n========== DATA QUALITY PASSED ==========\n")

finally:
    spark.stop()
