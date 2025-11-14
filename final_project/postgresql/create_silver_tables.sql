-- PostgreSQL SQL: Load Parquet data from Spark to PostgreSQL
-- This script loads silver layer Parquet files into PostgreSQL tables
-- Run this after Spark ETL jobs complete

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.customers CASCADE;
CREATE TABLE silver.customers (
    client_id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    registration_date DATE,
    state VARCHAR(100)
);

DROP TABLE IF EXISTS silver.sales CASCADE;
CREATE TABLE silver.sales (
    client_id INTEGER,
    purchase_date DATE,
    product_name VARCHAR(255),
    price DECIMAL(10, 2),
    PRIMARY KEY (client_id, purchase_date, product_name)
);

DROP TABLE IF EXISTS silver.user_profiles CASCADE;
CREATE TABLE silver.user_profiles (
    email VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    state VARCHAR(100),
    birth_date DATE,
    phone_number VARCHAR(50)
);

CREATE INDEX idx_sales_client_id ON silver.sales(client_id);
CREATE INDEX idx_sales_purchase_date ON silver.sales(purchase_date);
CREATE INDEX idx_customers_email ON silver.customers(email);
CREATE INDEX idx_user_profiles_email ON silver.user_profiles(email);



