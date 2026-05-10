-- ============================================
-- 1. View المرضى
-- ============================================
CREATE VIEW VW_PATIENTS AS
SELECT DISTINCT
    patient_name,
    Age as age,
    Gender as gender,
    blood_type,
    age_group
FROM healthcare_lakehouse.healthcare_silver;

-- ============================================
-- 2. View المستشفيات
-- ============================================
CREATE VIEW VW_HOSPITALS AS
SELECT DISTINCT
    hospital_name,
    doctor_name
FROM healthcare_lakehouse.healthcare_silver;

-- ============================================
-- 3. View الأمراض
-- ============================================
CREATE VIEW VW_CONDITIONS AS
SELECT DISTINCT
    medical_condition,
    medication_name
FROM healthcare_lakehouse.healthcare_silver;

-- ============================================
-- 4. View الدخول والخروج
-- ============================================
CREATE VIEW VW_ADMISSIONS AS
SELECT
    patient_name,
    hospital_name,
    medical_condition,
    admission_date,
    discharge_date,
    length_of_stay,
    billing_amount,
    admission_type,
    test_results,
    insurance_provider,
    admission_season
FROM healthcare_lakehouse.healthcare_silver;

-- ============================================
-- 5. الإنذار المبكر
-- ============================================
CREATE VIEW VW_EARLY_WARNING AS
SELECT 
    medical_condition,
    COUNT(*) as total_cases,
    AVG(CAST(billing_amount AS FLOAT)) as avg_cost,
    AVG(CAST(length_of_stay AS FLOAT)) as avg_stay,
    CASE 
        WHEN COUNT(*) > 5000 THEN 'High Risk'
        WHEN COUNT(*) > 2000 THEN 'Warning'
        ELSE 'Normal'
    END as warning_level
FROM healthcare_lakehouse.healthcare_silver
GROUP BY medical_condition;

-- ============================================
-- 6. تتبع الأوبئة
-- ============================================
CREATE VIEW VW_EPIDEMIC_TRACKING AS
SELECT 
    medical_condition,
    admission_season,
    COUNT(*) as cases_count,
    AVG(CAST(length_of_stay AS FLOAT)) as avg_stay
FROM healthcare_lakehouse.healthcare_silver
GROUP BY medical_condition, admission_season;

-- ============================================
--  Dimension Tables
-- ============================================

-- dim_patient
CREATE VIEW dim_patient AS
SELECT DISTINCT
    patient_name,
    age,
    gender,
    age_group,
    blood_type,
    length_of_stay
FROM healthcare_lakehouse.healthcare_silver;

-- dim_doctor
CREATE VIEW dim_doctor AS
SELECT DISTINCT
    doctor_name
FROM healthcare_lakehouse.healthcare_silver;

-- dim_hospital
CREATE VIEW dim_hospital AS
SELECT DISTINCT
    hospital_name
FROM healthcare_lakehouse.healthcare_silver;

-- dim_condition
CREATE VIEW dim_condition AS
SELECT DISTINCT
    medical_condition
FROM healthcare_lakehouse.healthcare_silver;

-- dim_insurance
CREATE VIEW dim_insurance AS
SELECT DISTINCT
    insurance_provider
FROM healthcare_lakehouse.healthcare_silver;

-- dim_date
CREATE VIEW dim_date AS
SELECT DISTINCT
    admission_date as full_date,
    YEAR(admission_date) as year,
    MONTH(admission_date) as month,
    DAY(admission_date) as day
FROM healthcare_lakehouse.healthcare_silver;

-- ============================================
-- - Fact Table
-- ============================================
CREATE VIEW fact_admissions AS
SELECT
    p.patient_name,
    d.doctor_name,
    h.hospital_name,
    c.medical_condition,
    i.insurance_provider,
    dt.full_date as admission_date,
    s.billing_amount,
    s.length_of_stay
FROM healthcare_lakehouse.healthcare_silver s
JOIN dim_patient p ON s.patient_name = p.patient_name
JOIN dim_doctor d ON s.doctor_name = d.doctor_name
JOIN dim_hospital h ON s.hospital_name = h.hospital_name
JOIN dim_condition c ON s.medical_condition = c.medical_condition
JOIN dim_insurance i ON s.insurance_provider = i.insurance_provider
JOIN dim_date dt ON s.admission_date = dt.full_date;