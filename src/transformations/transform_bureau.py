from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


spark = (
    SparkSession.builder
    .appName("BankingBureauTransformation")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/bureau.csv"
output_path = "data/processed/bureau"

# Read source
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

# Select required columns
bureau_df = df.select(
    "SK_ID_CURR",
    "SK_ID_BUREAU",
    "CREDIT_ACTIVE",
    "CREDIT_CURRENCY",
    "DAYS_CREDIT",
    "CREDIT_DAY_OVERDUE",
    "AMT_CREDIT_SUM",
    "AMT_CREDIT_SUM_DEBT",
    "AMT_CREDIT_SUM_OVERDUE",
    "CREDIT_TYPE",
)

# Aggregate bureau information at customer level
bureau_aggregated = (
    bureau_df
    .groupBy("SK_ID_CURR")
    .agg(
        count("SK_ID_BUREAU").alias("TOTAL_BUREAU_ACCOUNTS"),
        spark_sum("AMT_CREDIT_SUM").alias("TOTAL_CREDIT"),
        spark_sum("AMT_CREDIT_SUM_DEBT").alias("TOTAL_DEBT"),
        spark_sum("AMT_CREDIT_SUM_OVERDUE").alias("TOTAL_OVERDUE"),
    )
)

bureau_aggregated.show(10, truncate=False)

bureau_aggregated.printSchema()

# Write as Parquet
bureau_aggregated.write.mode("overwrite").parquet(output_path)

spark.stop()