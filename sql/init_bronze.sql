-- Create Bronze Schema if it doesn't exist (optional, or just use public)
-- CREATE SCHEMA IF NOT EXISTS bronze; 

-- Customer Table
CREATE TABLE IF NOT EXISTS bronze_customer (
    customer_id INT,
    customer_birth_date VARCHAR(50), -- Keeping as varchar for bronze to avoid date parsing errors initially
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    customer_phone VARCHAR(50),
    customer_address TEXT,
    customer_country VARCHAR(100),
    customer_city VARCHAR(100),
    customer_zip VARCHAR(20),
    file_name VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store Table
CREATE TABLE IF NOT EXISTS bronze_store (
    store_id INT,
    store_name VARCHAR(255),
    store_type VARCHAR(50),
    store_country VARCHAR(100),
    store_city VARCHAR(100),
    store_address TEXT,
    store_open_date VARCHAR(50), -- Keeping as varchar for bronze
    store_phone VARCHAR(50),
    store_area VARCHAR(50),
    store_manager VARCHAR(255),
    file_name VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Menu Table
CREATE TABLE IF NOT EXISTS bronze_menu (
    product_id INT,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    unit_price NUMERIC(10, 2),
    product_description TEXT,
    file_name VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
