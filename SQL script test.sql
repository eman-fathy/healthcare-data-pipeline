USE HEALTHCARE_DW;

-- test count of patients
SELECT COUNT(*) as total_patients FROM VW_PATIENTS;

-- test no null
SELECT COUNT(*) as null_names 
FROM VW_PATIENTS 
WHERE patient_name IS NULL;

-- test What are the most common diseases ?
SELECT medical_condition, COUNT(*) as cases
FROM VW_ADMISSIONS
GROUP BY medical_condition
ORDER BY cases DESC;

-- test avg of cost 
SELECT AVG(billing_amount) as avg_cost
FROM VW_ADMISSIONS;

-- test early warning
SELECT * FROM VW_EARLY_WARNING
ORDER BY total_cases DESC;