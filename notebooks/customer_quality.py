# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", IntegerType(), True),
])

records = [
    ("C101", "Anaya", "Active", 1200),
    ("C102", "Rahul", "Paused", 850),
    ("C103", "Riya", "Active", -100),
    ("", "Meera", "Active", 400),
]

customers = spark.createDataFrame(records, schema)

result = customers.withColumn(
    "validation_result",
    F.when(
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == ""),
        F.lit("REJECT")
    ).when(
        F.col("amount") < 0,
        F.lit("REJECT")
    ).otherwise(F.lit("VALID"))
)

result = customers.withColumn(
    "validation_result",
    F.when(
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == ""),
        F.lit("REJECT")
    ).when(
        F.col("amount") < 0,
        F.lit("REJECT")
    ).when(
        ~F.col("status").isin("Active", "Inactive"),
        F.lit("REJECT")
    ).otherwise(F.lit("VALID"))
)

result.orderBy("customer_id").show(truncate=False)

valid_count = result.filter(F.col("validation_result") == "VALID").count()
rejected_count = result.filter(F.col("validation_result") == "REJECT").count()

print("Valid records:", valid_count)
print("Rejected records:", rejected_count)
print("All assertions passed")

assert valid_count == 1, "Expected exactly one valid record"
assert rejected_count == 3, "Expected exactly three rejected records"