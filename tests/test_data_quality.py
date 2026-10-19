
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def test_invalid_income_records_are_detected():
    spark = (
        SparkSession.builder
        .appName("BankingDataQualityTests")
        .master("local[1]")
        .getOrCreate()
    )

    try:
        data = [
            (100001, 50000.0, 100000.0),
            (100002, -5000.0, 100000.0),
            (100003, 0.0, 50000.0),
        ]

        df = spark.createDataFrame(
            data,
            ["SK_ID_CURR", "AMT_INCOME_TOTAL", "AMT_CREDIT"],
        )

        invalid_records = df.filter(
            col("AMT_INCOME_TOTAL") <= 0
        )

        assert invalid_records.count() == 2

    finally:
        spark.stop()
