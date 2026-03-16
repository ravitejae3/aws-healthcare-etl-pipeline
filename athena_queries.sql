-- ============================================================
-- athena_queries.sql
-- ============================================================
-- Run these in Amazon Athena after the Glue job completes.
-- Before running: Make sure you've set the Athena query results
-- location to: s3://raviteja-healthcare-pipeline/athena-results/
--
-- IMPORTANT: Replace 'healthcare_processed' with your actual
-- Glue table name if it differs.
-- ============================================================


-- ── QUERY 1: Patient count by condition ──────────────────
-- Great for a README screenshot — shows the pipeline produced real results

SELECT
    condition,
    COUNT(*)                          AS patient_count,
    ROUND(AVG(billing_amount), 2)     AS avg_billing_usd,
    ROUND(AVG(los_days), 1)           AS avg_length_of_stay,
    ROUND(AVG(age), 1)                AS avg_age
FROM healthcare_processed
GROUP BY condition
ORDER BY patient_count DESC;


-- ── QUERY 2: High-cost patients by state ─────────────────
SELECT
    state,
    COUNT(*)                          AS high_cost_patients,
    ROUND(SUM(billing_amount), 2)     AS total_billing_usd,
    ROUND(AVG(billing_amount), 2)     AS avg_billing_usd
FROM healthcare_processed
WHERE high_cost_flag = true
GROUP BY state
ORDER BY total_billing_usd DESC;


-- ── QUERY 3: Insurance type breakdown ────────────────────
SELECT
    insurance,
    COUNT(*)                          AS patient_count,
    ROUND(AVG(billing_amount), 2)     AS avg_billing,
    COUNT(CASE WHEN high_cost_flag = true THEN 1 END) AS high_cost_count
FROM healthcare_processed
GROUP BY insurance
ORDER BY patient_count DESC;


-- ── QUERY 4: Monthly admissions trend ────────────────────
SELECT
    admission_year,
    admission_month,
    COUNT(*)                          AS admissions,
    ROUND(AVG(billing_amount), 2)     AS avg_billing
FROM healthcare_processed
GROUP BY admission_year, admission_month
ORDER BY admission_year, admission_month;


-- ── QUERY 5: Age group distribution by condition ─────────
SELECT
    condition,
    age_group,
    COUNT(*)                          AS patient_count,
    ROUND(AVG(billing_amount), 2)     AS avg_billing
FROM healthcare_processed
GROUP BY condition, age_group
ORDER BY condition, age_group;


-- ── QUERY 6: Long stay patients ──────────────────────────
SELECT
    hospital,
    COUNT(*)                                          AS total_patients,
    COUNT(CASE WHEN long_stay_flag = true THEN 1 END) AS long_stay_count,
    ROUND(AVG(los_days), 1)                           AS avg_los_days,
    ROUND(AVG(billing_amount), 2)                     AS avg_billing
FROM healthcare_processed
GROUP BY hospital
ORDER BY avg_los_days DESC;
