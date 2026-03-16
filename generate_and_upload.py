"""
generate_and_upload.py
======================
Generates 10,000 synthetic patient records and uploads them to S3.
Run this first before setting up the Glue job.

Usage:
    python src/generate_and_upload.py
"""

import boto3
import pandas as pd
from faker import Faker
import random
import io
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)

# ── CONFIG ─────────────────────────────────────────────
BUCKET_NAME = "raviteja-healthcare-pipeline"
S3_KEY      = "raw/healthcare_data.csv"
NUM_RECORDS = 10_000
# ───────────────────────────────────────────────────────

CONDITIONS   = ["Diabetes", "Hypertension", "Asthma", "Cancer", "Heart Disease",
                "Obesity", "Depression", "Arthritis", "COPD", "Stroke"]
MEDICATIONS  = ["Metformin", "Lisinopril", "Atorvastatin", "Albuterol",
                "Omeprazole", "Amlodipine", "Sertraline", "Ibuprofen"]
BLOOD_TYPES  = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
HOSPITALS    = ["Providence Health", "Seattle Medical Center", "Northwest Hospital",
                "Pacific Medical", "Virginia Mason"]
STATES       = ["WA", "OR", "CA", "NY", "TX", "IL", "FL"]


def generate_patient_record():
    """Generate a single synthetic patient record."""
    age = random.randint(18, 90)
    admission = fake.date_between(start_date="-2y", end_date="today")
    discharge  = admission + timedelta(days=random.randint(1, 14))

    return {
        "patient_id":       fake.uuid4(),
        "first_name":       fake.first_name(),
        "last_name":        fake.last_name(),
        "age":              age,
        "gender":           random.choice(["Male", "Female", "Other"]),
        "blood_type":       random.choice(BLOOD_TYPES),
        "condition":        random.choice(CONDITIONS),
        "medication":       random.choice(MEDICATIONS),
        "hospital":         random.choice(HOSPITALS),
        "state":            random.choice(STATES),
        "admission_date":   str(admission),
        "discharge_date":   str(discharge),
        "los_days":         (discharge - admission).days,   # length of stay
        "billing_amount":   round(random.uniform(500, 50000), 2),
        "insurance":        random.choice(["Medicare", "Medicaid", "Private", "Uninsured"]),
        # Intentionally add some dirty data so the Glue job has something to clean
        "phone":            fake.phone_number() if random.random() > 0.1 else None,
        "zip_code":         fake.zipcode() if random.random() > 0.05 else "INVALID",
    }


def main():
    print(f"⚙️  Generating {NUM_RECORDS:,} patient records...")
    records = [generate_patient_record() for _ in range(NUM_RECORDS)]
    df = pd.DataFrame(records)

    print(f"✅ Generated {len(df):,} records")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Sample:\n{df.head(3).to_string()}\n")

    # Convert to CSV in memory (no temp file needed)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    print(f"📤 Uploading to s3://{BUCKET_NAME}/{S3_KEY} ...")
    s3 = boto3.client("s3")

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
            Body=csv_bytes,
            ContentType="text/csv"
        )
        print(f"✅ Uploaded healthcare_data.csv to s3://{BUCKET_NAME}/{S3_KEY}")
        print(f"   File size: {len(csv_bytes)/1024:.1f} KB")
        print(f"\n👉 Next step: Go to AWS Glue and create the ETL job using src/glue_etl_job.py")

    except Exception as e:
        print(f"❌ Upload failed: {e}")
        print("   Make sure you ran 'aws configure' with your credentials.")
        raise


if __name__ == "__main__":
    main()
