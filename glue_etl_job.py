"""
glue_etl_job.py
===============
AWS Glue ETL script — paste this into the Glue Script Editor in the AWS Console.

What this job does:
  1. Reads the raw CSV from S3
  2. Cleans dirty/invalid data (nulls, bad zip codes, etc.)
  3. Adds derived columns (age_group, high_cost_flag, etc.)
  4. Writes processed data back to S3 in Parquet format (partitioned by condition)

This is a real PySpark script running on AWS Glue infrastructure.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# ── CONFIG ─────────────────────────────────────────────
INPUT_PATH  = "s3://raviteja-healthcare-pipeline/raw/healthcare_data.csv"
OUTPUT_PATH = "s3://raviteja-healthcare-pipeline/processed/"
# ───────────────────────────────────────────────────────

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("📥 Reading raw healthcare data from S3...")

# Read raw CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
raw_count = df.count()
print(f"   Raw records: {raw_count:,}")

# ── STEP 1: DATA CLEANING ──────────────────────────────

print("🧹 Cleaning data...")

# Drop records with null patient_id or condition (critical fields)
df_clean = df.filter(
    F.col("patient_id").isNotNull() &
    F.col("condition").isNotNull() &
    F.col("age").isNotNull()
)

# Fix invalid zip codes — replace "INVALID" with null
df_clean = df_clean.withColumn(
    "zip_code",
    F.when(F.col("zip_code") == "INVALID", None).otherwise(F.col("zip_code"))
)

# Ensure age is in a valid range (18–110)
df_clean = df_clean.filter((F.col("age") >= 18) & (F.col("age") <= 110))

# Trim whitespace from string columns
for col_name in ["condition", "medication", "hospital", "state", "insurance", "gender"]:
    df_clean = df_clean.withColumn(col_name, F.trim(F.upper(F.col(col_name))))

# Standardize gender values
df_clean = df_clean.withColumn(
    "gender",
    F.when(F.col("gender").isin("MALE", "M"), "Male")
     .when(F.col("gender").isin("FEMALE", "F"), "Female")
     .otherwise("Other")
)

cleaned_count = df_clean.count()
dropped = raw_count - cleaned_count
print(f"   After cleaning: {cleaned_count:,} records ({dropped:,} dropped)")

# ── STEP 2: FEATURE ENGINEERING ───────────────────────

print("⚙️  Adding derived columns...")

# Age group buckets
df_enriched = df_clean.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "18-29")
     .when(F.col("age") < 45, "30-44")
     .when(F.col("age") < 60, "45-59")
     .when(F.col("age") < 75, "60-74")
     .otherwise("75+")
)

# High cost flag (>$20,000 billing)
df_enriched = df_enriched.withColumn(
    "high_cost_flag",
    F.when(F.col("billing_amount") > 20000, True).otherwise(False)
)

# Long stay flag (>7 days)
df_enriched = df_enriched.withColumn(
    "long_stay_flag",
    F.when(F.col("los_days") > 7, True).otherwise(False)
)

# Year and month from admission date
df_enriched = df_enriched.withColumn(
    "admission_year", F.year(F.to_date(F.col("admission_date")))
)
df_enriched = df_enriched.withColumn(
    "admission_month", F.month(F.to_date(F.col("admission_date")))
)

# Add processing timestamp
df_enriched = df_enriched.withColumn(
    "processed_at", F.current_timestamp()
)

# ── STEP 3: SUMMARY STATS (logged to Glue) ────────────

print("\n📊 Summary Statistics:")
df_enriched.groupBy("condition").agg(
    F.count("patient_id").alias("count"),
    F.round(F.avg("billing_amount"), 2).alias("avg_billing"),
    F.round(F.avg("los_days"), 1).alias("avg_los"),
    F.round(F.avg("age"), 1).alias("avg_age")
).orderBy(F.desc("count")).show(10, truncate=False)

# ── STEP 4: WRITE PROCESSED DATA ──────────────────────

print(f"\n💾 Writing processed data to {OUTPUT_PATH} ...")

df_enriched.write \
    .mode("overwrite") \
    .partitionBy("condition") \
    .parquet(OUTPUT_PATH)

print(f"✅ Glue ETL job complete!")
print(f"   Records written: {df_enriched.count():,}")
print(f"   Output path: {OUTPUT_PATH}")
print(f"\n👉 Next step: Query the data with Athena using src/athena_queries.sql")

job.commit()
