import yaml 
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.functions import col, when, avg, isnan, count
from pyspark.sql import functions as F


# === Load Config ===
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# === Start Spark session ===
spark = SparkSession.builder.appName("LoanRiskETL").enableHiveSupport().getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# === Load data ===
df = spark.read.option("header", True).csv("data/loan.csv", inferSchema=True)
df.show(2, truncate=False)
df.printSchema()

# === Data Cleaning and Normalization ===
 
# data type map
type_map = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "date": DateType()
}

# Cast columns to specified data types
for col_name, dtype in config["data_types"].items():
    spark_type = type_map.get(dtype)
    if spark_type:
        df1 = df.withColumn(col_name, col(col_name).cast(spark_type))
    else:
        print(f"Warning: No Spark type found for {col_name}: {dtype}")

df1.show(2, truncate=False) #dataframe after casting

# Rename Columns
for col_name, new_name in config["rename_columns"].items():
    df1 = df1.withColumnRenamed(col_name, new_name)
    print(f"Renamed column {col_name} to {new_name}")

df1.show(2, truncate=False) #dataframe after renaming columns

# Filter data 
current_date = spark.sql("SELECT current_date()").collect()[0][0]
filtered_df = df1.filter(col("application_date") <= current_date)

# Check for missing values
missing_cols=filtered_df.select(*[
    (
        F.count(F.when((F.isnan(c) | F.col(c).isNull()), c)) if t not in ("date")
        else F.count(F.when(F.col(c).isNull(), c))
    ).alias(c)
    for c, t in filtered_df.dtypes if c in config["check_missing_values"]
])

if missing_cols.count() > 0:
    print("Warning: Missing values found in the dataset. Please handle them before proceeding.")
    missing_cols.show()
else:
    print("No missing values found in the dataset.")

# Check for duplicates
clean_df = filtered_df.dropDuplicates()
duplicate_count = filtered_df.count() - clean_df.count()
if duplicate_count > 0:
    print(f"Warning: {duplicate_count} duplicate rows found and removed.")
else:
    print("No duplicate rows found in the dataset.")


# === Feature Engineering ===

clean_df = (clean_df
            .withColumn("loan_age", current_date - col("application_date"))
            .withColumn("effective_interest_spread", col("interest_rate") - col("base_interest_rate"))
            .withColumn("net_debt", col("total_liabilities") - col("savings_account_balance") - col("checking_account_balance"))
            .withColumn("total_debt", col("monthly_debt_payments") * 12)
            .withColumn("has_bankruptcy", col("bankruptcy_history") > 0)
            .withColumn("disposable_income", col("annual_income") - (col("monthly_debt_payments") * 12))
            .withColumn("loan_income_ratio", col("loan_amount") / col("annual_income"))
            .withColumn("income_per_dependent", col("annual_income") / (col("number_of_dependents") + 1))  # Avoid division by zero
            .withColumn("credit_score_band",
                        when(col("credit_score") < 580, "Poor")
                        .when((col("credit_score") >= 580) & (col("credit_score") < 670), "Fair")
                        .when((col("credit_score") >= 670) & (col("credit_score") < 740), "Good")
                        .when((col("credit_score") >= 740) & (col("credit_score") < 800), "Very Good")
                        .otherwise("Excellent")
                        )
            .withColumn("income_band",
                        when(col("annual_income") < 40000, "Low")
                        .when((col("annual_income") >= 40000) & (col("annual_income") < 100000), "Mid")
                        .otherwise("High")
                        )
            .withColumn("net_worth_computed", col("total_assets") - col("total_liabilities"))
            .withColumn("net_worth_band",
                        when(col("net_worth_computed") < 0, "Negative")
                        .when((col("net_worth_computed") >= 0) & (col("net_worth_computed") < 50000), "Low")
                        .when((col("net_worth_computed") >= 50000) & (col("net_worth_computed") < 200000), "Medium")
                        .otherwise("High")
                        )

)
# Show the cleaned DataFrame
clean_df.show(2, truncate=False)
# Print schema of the cleaned DataFrame
clean_df.printSchema()

# === Quality Checks ===
flagged_issues = (clean_df
                  .withColumn("credit_score_flag", when((col("credit_score") >= 300) & (col("credit_score") <= 850), True).otherwise(False))
                  .withColumn("income_flag", when(col("annual_income") > 0, True).otherwise(False))
                  .withColumn("loan_to_asset_flag", col("loan_amount") <= col("total_assets"))
).filter((col("credit_score_flag") == False) | (col("income_flag") == False) | (col("loan_to_asset_flag") == False))


if flagged_issues.count() > 0:
    print("Warning: Issues found in credit score or income data.")
else:
    print("No issues found in credit score or income data.")


# === Save Output ===

# Save locally 
clean_df.write.mode("overwrite").option("header", "true").parquet(config["output_path"] + '/' + current_date.strftime("%Y-%m-%d") + '/' + 'cleaned_data/')
flagged_issues.write.mode("overwrite").option("header", "true").parquet(config["output_path"]+ '/' + current_date.strftime("%Y-%m-%d") + '/' + 'flagged_issues/')

#save to aws
# clean_df.write.mode("overwrite").option("header", "true").parquet("s3://your-bucket/path/")
# flagged_issues.write.mode("overwrite").option("header", "true").parquet("s3://your-bucket/path/")

# === Stop Spark session ===
spark.stop()

