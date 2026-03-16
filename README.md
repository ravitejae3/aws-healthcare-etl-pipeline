# AWS Healthcare Batch ETL Pipeline

End-to-end batch data pipeline on AWS that ingests synthetic healthcare data, runs a PySpark-based Glue ETL job to clean and transform it, and surfaces insights through Athena SQL queries.

Built to demonstrate AWS data engineering skills across S3, Glue, and Athena — matching the kind of work done on clinical analytics pipelines in healthcare environments.

---

## Architecture

```
generate_and_upload.py  ──►  S3 (raw/)
                                  │
                                  ▼
                         AWS Glue ETL Job (PySpark)
                         - Drop nulls & invalid records
                         - Standardize & enrich fields
                         - Add age_group, cost flags
                                  │
                                  ▼
                         S3 (processed/) — Parquet, partitioned by condition
                                  │
                                  ▼
                         Amazon Athena (SQL queries)
```

---

## Tech Stack

- **Python 3.10** — data generation and S3 upload
- **AWS S3** — raw and processed data storage
- **AWS Glue** — managed PySpark ETL (cleaning + transformation)
- **Amazon Athena** — serverless SQL over S3
- **Pandas / Faker** — synthetic data generation
- **Parquet** — columnar storage format

---

## What the Pipeline Does

1. **Generates** 10,000 synthetic patient records (conditions, billing, medications, demographics) with intentionally messy data
2. **Uploads** the CSV to S3 raw layer via boto3
3. **Cleans** invalid records in Glue: nulls, bad zip codes, out-of-range ages, inconsistent strings
4. **Enriches** data with derived fields: age_group, high_cost_flag, long_stay_flag, admission_year/month
5. **Writes** processed data back to S3 as Parquet, partitioned by condition
6. **Queries** the processed data with Athena SQL for analytics

---

## Setup & Run

### Prerequisites
- AWS account (free tier)
- Python 3.10+
- AWS CLI configured (`aws configure`)

### Install dependencies
```bash
pip install -r requirements.txt
```

### Step 1 — Generate data and upload to S3
```bash
python src/generate_and_upload.py
```

### Step 2 — Run the Glue ETL job
Paste `src/glue_etl_job.py` into the AWS Glue Script Editor and run it.
See the project guide for step-by-step AWS Console instructions.

### Step 3 — Query with Athena
Run the queries in `src/athena_queries.sql` in the Athena query editor.

---

## Sample Output

| condition | patient_count | avg_billing_usd | avg_los |
|-----------|--------------|-----------------|---------|
| Diabetes | 1,024 | $18,432.50 | 5.2 |
| Hypertension | 987 | $14,891.20 | 4.8 |
| Cancer | 1,102 | $34,201.75 | 8.1 |

---

## Project Structure

```
project1-aws-healthcare-etl/
├── src/
│   ├── generate_and_upload.py   # Generate + upload to S3
│   ├── glue_etl_job.py          # PySpark Glue ETL script
│   └── athena_queries.sql       # Analytics SQL queries
├── requirements.txt
└── README.md
```
