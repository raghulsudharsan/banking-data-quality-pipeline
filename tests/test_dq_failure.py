from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("DQFailureTest")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (100001, 50000.0, 100000.0),
    (100002, -5000.0, 100000.0),   # INVALID INCOME
]

df = spark.createDataFrame(
    data,
    ["SK_ID_CURR", "AMT_INCOME_TOTAL", "AMT_CREDIT"]
)

invalid_income = (
    df
    .filter(col("AMT_INCOME_TOTAL") <= 0)
    .count()
)

print(f"Invalid income records: {invalid_income}")

if invalid_income > 0:
    raise Exception(
        f"Data Quality Check Failed: "
        f"{invalid_income} records have invalid income"
    )

spark.stop()