-- PostgreSQL SQL: enrich_user_profiles (Local)
-- Pipeline: silver → gold
-- Description: Enrich customers data with user_profiles data
-- Note: This assumes silver tables are loaded from Parquet files into PostgreSQL

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.user_profiles_enriched;

CREATE TABLE gold.user_profiles_enriched (
    client_id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    registration_date DATE,
    state VARCHAR(100),
    birth_date DATE,
    phone_number VARCHAR(50),
    age INTEGER
);

-- Insert enriched data using MERGE pattern (UPSERT)
-- Fill missing first_name, last_name, state from user_profiles
-- Add additional fields (phone_number, birth_date, age) from user_profiles
INSERT INTO gold.user_profiles_enriched
SELECT 
    c.client_id,
    COALESCE(NULLIF(TRIM(c.first_name), ''), up.first_name) AS first_name,
    COALESCE(NULLIF(TRIM(c.last_name), ''), up.last_name) AS last_name,
    c.email,
    c.registration_date,
    COALESCE(NULLIF(TRIM(c.state), ''), up.state) AS state,
    up.birth_date,
    up.phone_number,
    EXTRACT(YEAR FROM AGE(up.birth_date)) AS age
FROM silver.customers c
LEFT JOIN silver.user_profiles up
    ON LOWER(TRIM(c.email)) = LOWER(TRIM(up.email));



