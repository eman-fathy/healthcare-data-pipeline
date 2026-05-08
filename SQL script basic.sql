CREATE DATABASE HEALTHCARE_DW

USE HEALTHCARE_DW;

CREATE VIEW VW_PATIENTS AS
SELECT DISTINCT
    patient_name,
    Age as age,
    Gender as gender,
    blood_type,
    age_group
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result;

SELECT TOP 10 * FROM VW_PATIENTS

CREATE VIEW VW_HOSPITALS AS
SELECT DISTINCT
    hospital_name,
    doctor_name
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result;

CREATE VIEW VW_CONDITIONS AS
SELECT DISTINCT
    medical_condition,
    medication_name
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result;

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
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result;

SELECT TOP 10 * FROM VW_HOSPITALS;
SELECT TOP 10 * FROM VW_CONDITIONS;
SELECT TOP 10 * FROM VW_ADMISSIONS;


