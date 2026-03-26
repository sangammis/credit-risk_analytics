-- CUSTOMER DIM
CREATE TABLE customer_dim AS
SELECT DISTINCT
    customer_id,
    age,
    income,
    city
FROM loan_customers;


-- EMPLOYMENT DIM
CREATE TABLE employment_dim AS
SELECT DISTINCT
    employment_type
FROM loan_customers;


-- CREDIT DIM
CREATE TABLE credit_dim AS
SELECT DISTINCT
    credit_score,
    CASE 
        WHEN credit_score < 600 THEN 'Poor'
        WHEN credit_score BETWEEN 600 AND 750 THEN 'Good'
        ELSE 'Excellent'
    END AS credit_band
FROM loan_customers;


-- FACT TABLE
CREATE TABLE loan_fact AS
SELECT 
    customer_id,
    loan_amount,
    default_flag
FROM loan_customers;


SELECT 
    c.city,
    e.employment_type,
    ROUND(AVG(f.default_flag)*100,2) AS default_rate
FROM loan_fact f
JOIN customer_dim c ON f.customer_id = c.customer_id
JOIN employment_dim e ON f.employment_type = e.employment_type
GROUP BY c.city, e.employment_type
ORDER BY default_rate DESC;
