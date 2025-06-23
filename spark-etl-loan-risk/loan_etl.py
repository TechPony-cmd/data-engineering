import yaml 
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.functions import col, when, avg
from pyspark.sql import functions as F

# === Load Config ===
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# === Start Spark session ===
spark = SparkSession.builder.appName("LoanRiskETL").getOrCreate()

# === Load data ===
df = spark.read.option("header", True).csv("data/loan.csv", inferSchema=True)
df.show(2, truncate=False)
df.printSchema()

# === Data Cleaning and Transformation ===

# Data Type Map
type_map = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType()
}

# Cast Columns to Specified Data Types
for col_name, dtype in config["data_types"].items():
    spark_type = type_map.get(dtype)
    if spark_type:
        df1 = df.withColumn(col_name, col(col_name).cast(spark_type))
    else:
        print(f"Warning: No Spark type found for {col_name}: {dtype}")

df.show(2, truncate=False) #dataframe after casting

# Rename Columns
for col_name, new_name in config["rename_columns"].items():
    df1 = df1.withColumnRenamed(col_name, new_name)

df1.show(2, truncate=False) #dataframe after renaming columns

# === aggregate Data ===

# Bucketing for Credit Score, Income, and Net Worth

# Credit Score Buckets (FICO-style)
df1 = df1.withColumn(
    "credit_score_band",
      when(col("credit_score") < 580, "Poor")
      .when((col("credit_score") >= 580) & (col("credit_score") < 670), "Fair")
      .when((col("credit_score") >= 670) & (col("credit_score") < 740), "Good")
      .when((col("credit_score") >= 740) & (col("credit_score") < 800), "Very Good")
      .otherwise("Excellent")
)

# Income Buckets
df1 = df1.withColumn(
    "income_band",
    when(col("annual_income") < 40000, "Low")
    .when((col("annual_income") >= 40000) & (col("annual_income") < 100000), "Mid")
    .otherwise("High")
)

# Net Worth (Assets - Liabilities)
df1 = df1.withColumn("net_worth_computed", col("total_assets") - col("total_liabilities"))
df1 = df1.withColumn(
    "net_worth_band",
    when(col("net_worth_computed") < 0, "Negative")
    .when((col("net_worth_computed") >= 0) & (col("net_worth_computed") < 50000), "Low")
    .when((col("net_worth_computed") >= 50000) & (col("net_worth_computed") < 200000), "Medium")
    .otherwise("High")
)
df1.printSchema()

# - **Approval Rate** by credit score, income, and loan purpose
# - 💳 **Default Rate** based on FICO-style score bands
# - 💰 **Loan-to-Income Ratio** and **Monthly Payment Ratios**
# - 📈 **Average Loan Amount**, Interest Rate, and Net Worth
# - 🧮 **Credit Card Utilization**, Tenure, and Risk Scores
# - 🏦 Aggregations saved to Parquet for downstream use


avg_approval_rate = (
    df1
    .groupBy("credit_score_band", "income_band", "loan_purpose")
    .agg(
        F.avg("loan_approved").alias("approval_rate")
        )
    .orderBy("credit_score_band", "income_band", "loan_purpose")
)
avg_approval_rate.show(4, truncate=False)


# === Save Output ===
df1.write.mode("overwrite").parquet(config["output_path"]["non_aggregated"])
avg_approval_rate.write.mode("overwrite").csv(config["output_path"]["aggregated"]['avg_approval_rate'])

# === Stop Spark session ===
spark.stop()