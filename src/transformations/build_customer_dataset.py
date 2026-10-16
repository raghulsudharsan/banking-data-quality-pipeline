from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum

application_path = "hdfs://localhost:9000/banking/processed/application"
bureau_valid_path = "hdfs://localhost:9000/banking/processed/bureau_valid"
output_path = "hdfs://localhost:9000/banking/processed/customer"

spark = (
    SparkSession.builder
    .appName("BuildCustomerDataset")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

try:
    application_df = spark.read.parquet(application_path)
    bureau_df = spark.read.parquet(bureau_valid_path)

    bureau_aggregated = bureau_df.groupBy("SK_ID_CURR").agg(
        count("SK_ID_BUREAU").alias("TOTAL_BUREAU_ACCOUNTS"),
        spark_sum("AMT_CREDIT_SUM").alias("TOTAL_CREDIT"),
        spark_sum("AMT_CREDIT_SUM_DEBT").alias("TOTAL_DEBT"),
        spark_sum("AMT_CREDIT_SUM_OVERDUE").alias("TOTAL_OVERDUE"),
    )

    customer_df = application_df.join(
        bureau_aggregated,
        on="SK_ID_CURR",
        how="left",
    )

    customer_df = customer_df.fillna({
        "TOTAL_BUREAU_ACCOUNTS": 0,
        "TOTAL_CREDIT": 0,
        "TOTAL_DEBT": 0,
        "TOTAL_OVERDUE": 0,
    })

    application_count = application_df.count()
    bureau_count = bureau_df.count()
    customer_count = customer_df.count()

    print("\nCustomer Dataset Summary")
    print("------------------------")
    print(f"Application records : {application_count}")
    print(f"Valid bureau records: {bureau_count}")
    print(f"Customer records    : {customer_count}")

    customer_df.write.mode("overwrite").parquet(output_path)

    print(f"Customer dataset written to HDFS: {output_path}")

finally:
    spark.stop()
