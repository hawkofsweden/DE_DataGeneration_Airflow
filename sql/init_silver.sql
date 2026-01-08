-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Modify Bronze Tables (Add Surrogate Key)
DO $$ 
BEGIN 
    BEGIN
        ALTER TABLE bronze_customer ADD COLUMN bronze_id SERIAL PRIMARY KEY;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;
    
    BEGIN
        ALTER TABLE bronze_store ADD COLUMN bronze_id SERIAL PRIMARY KEY;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE bronze_menu ADD COLUMN bronze_id SERIAL PRIMARY KEY;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;
END $$;

-- 2. Create Silver Tables

-- Customer
CREATE TABLE IF NOT EXISTS silver_customer (
    customer_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bronze_id INT, -- FK to Bronze
    original_id INT,
    customer_name TEXT,
    customer_email TEXT,
    customer_phone TEXT,
    customer_address TEXT,
    customer_country TEXT,
    customer_city TEXT,
    customer_zip TEXT,
    customer_birth_date DATE,
    age INT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_customer_bronze_id ON silver_customer(bronze_id);


-- Store
CREATE TABLE IF NOT EXISTS silver_store (
    store_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bronze_id INT,
    original_id INT,
    store_name TEXT,
    store_type TEXT,
    store_country TEXT,
    store_city TEXT,
    store_address TEXT,
    store_open_date DATE,
    store_manager TEXT,
    store_phone TEXT,
    store_area TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_store_bronze_id ON silver_store(bronze_id);


-- Menu
CREATE TABLE IF NOT EXISTS silver_menu (
    product_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bronze_id INT,
    original_id INT,
    product_name TEXT,
    product_category TEXT,
    unit_price NUMERIC(10, 2),
    product_description TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_silver_menu_bronze_id ON silver_menu(bronze_id);


-- 3. Stored Procedures

-- Customer Procedure
CREATE OR REPLACE PROCEDURE sp_process_silver_customer(p_force_refresh BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_force_refresh THEN
        TRUNCATE TABLE silver_customer;
    END IF;

    INSERT INTO silver_customer (
        bronze_id, original_id, customer_name, customer_email, customer_phone,
        customer_address, customer_country, customer_city, customer_zip,
        customer_birth_date, age
    )
    SELECT 
        b.bronze_id,
        b.customer_id,
        b.customer_name,
        b.customer_email,
        -- Simple Standardization: Remove non-digit chars from phone
        REGEXP_REPLACE(b.customer_phone, '[^0-9+]', '', 'g'),
        b.customer_address,
        b.customer_country,
        b.customer_city,
        b.customer_zip,
        TO_DATE(b.customer_birth_date, 'YYYY-MM-DD'),
        EXTRACT(YEAR FROM age(CURRENT_DATE, TO_DATE(b.customer_birth_date, 'YYYY-MM-DD')))
    FROM bronze_customer b
    WHERE NOT EXISTS (
        SELECT 1 FROM silver_customer s WHERE s.bronze_id = b.bronze_id
    );
    
    RAISE NOTICE 'Processed Customer Data';
END;
$$;

-- Store Procedure
CREATE OR REPLACE PROCEDURE sp_process_silver_store(p_force_refresh BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_force_refresh THEN
        TRUNCATE TABLE silver_store;
    END IF;

    INSERT INTO silver_store (
        bronze_id, original_id, store_name, store_type, store_country,
        store_city, store_address, store_open_date, store_manager,
        store_phone, store_area
    )
    SELECT 
        b.bronze_id,
        b.store_id,
        b.store_name,
        b.store_type,
        b.store_country,
        b.store_city,
        b.store_address,
        -- Handle potential date format issues or assume YYYY-MM-DD/Timestamp
        CASE 
            WHEN b.store_open_date ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(b.store_open_date, 'YYYY-MM-DD')
            ELSE NULL -- Handle gracefully or add specific format logic
        END,
        b.store_manager,
        b.store_phone,
        b.store_area
    FROM bronze_store b
    WHERE NOT EXISTS (
        SELECT 1 FROM silver_store s WHERE s.bronze_id = b.bronze_id
    );

    RAISE NOTICE 'Processed Store Data';
END;
$$;

-- Menu Procedure
CREATE OR REPLACE PROCEDURE sp_process_silver_menu(p_force_refresh BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_force_refresh THEN
        TRUNCATE TABLE silver_menu;
    END IF;

    INSERT INTO silver_menu (
        bronze_id, original_id, product_name, product_category,
        unit_price, product_description
    )
    SELECT 
        b.bronze_id,
        b.product_id,
        b.product_name,
        b.product_category,
        b.unit_price,
        b.product_description
    FROM bronze_menu b
    WHERE NOT EXISTS (
        SELECT 1 FROM silver_menu s WHERE s.bronze_id = b.bronze_id
    );

    RAISE NOTICE 'Processed Menu Data';
END;
$$;
