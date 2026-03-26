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
    default_flag,
    credit_score,
    employment_type
FROM loan_customers;
