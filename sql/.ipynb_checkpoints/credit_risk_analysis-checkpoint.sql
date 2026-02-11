-- Credit Risk Analysis - MySQL

-- Overall default rate
SELECT 
    COUNT(*) AS total_customers,
    SUM(default_flag) AS total_defaults,
    ROUND(AVG(default_flag)*100,2) AS default_rate
FROM loan_customers;

-- Default by employment type
SELECT 
    employment_type,
    COUNT(*) AS customers,
    ROUND(AVG(default_flag)*100,2) AS default_rate
FROM loan_customers
GROUP BY employment_type
ORDER BY default_rate DESC;

-- Credit score band analysis
SELECT 
CASE 
    WHEN credit_score < 600 THEN 'Poor'
    WHEN credit_score BETWEEN 600 AND 700 THEN 'Average'
    ELSE 'Good'
END AS score_band,
COUNT(*) AS customers,
ROUND(AVG(default_flag)*100,2) AS default_rate
FROM loan_customers
GROUP BY score_band;


