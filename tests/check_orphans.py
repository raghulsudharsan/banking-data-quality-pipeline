from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("CheckOrphanBureauRecords")
    .master("local[*]")
    .getOrCreate()
)

application_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/application_train.csv")
)

bureau_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/bureau.csv")
)

application_ids = (
    application_df
    .select("SK_ID_CURR")
    .distinct()
)

orphans = (
    bureau_df
    .select("SK_ID_CURR")
    .distinct()
    .join(
        application_ids,
        on="SK_ID_CURR",
        how="left_anti"
    )
)

print(f"Orphan bureau customer IDs: {orphans.count()}")

orphans.show(20, truncate=False)

spark.stop()
