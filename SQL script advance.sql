USE HEALTHCARE_DW;
-- 🚨 إنذار مبكر: أكثر الأمراض انتشاراً
CREATE VIEW VW_EARLY_WARNING AS
SELECT 
    medical_condition,
    COUNT(*) as total_cases,
    AVG(billing_amount) as avg_cost,
    AVG(length_of_stay) as avg_stay,
    CASE 
        WHEN COUNT(*) > 5000 THEN 'خطر عالي 🔴'
        WHEN COUNT(*) > 2000 THEN 'تحذير 🟡'
        ELSE 'طبيعي 🟢'
    END as warning_level
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result
GROUP BY medical_condition;

-- 🔴 تتبع الأوبئة: انتشار الأمراض بالموسم
CREATE VIEW VW_EPIDEMIC_TRACKING AS
SELECT 
    medical_condition,
    admission_season,
    COUNT(*) as cases_count,
    AVG(length_of_stay) as avg_stay
FROM OPENROWSET(
    BULK 'https://healthcaredatalake01.dfs.core.windows.net/healthcare-data/silver/healthcare_cleaned/',
    FORMAT = 'DELTA'
) AS result
GROUP BY medical_condition, admission_season;