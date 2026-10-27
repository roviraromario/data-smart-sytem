CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.online_retail_raw (
    source_name TEXT NOT NULL,
    source_row_id BIGINT NOT NULL,
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity TEXT,
    invoice_date TEXT,
    source_date_format TEXT NOT NULL DEFAULT 'month_first',
    unit_price TEXT,
    customer_id TEXT,
    country TEXT,
    PRIMARY KEY (source_name, source_row_id)
);

ALTER TABLE bronze.online_retail_raw
    ADD COLUMN IF NOT EXISTS source_date_format TEXT NOT NULL DEFAULT 'month_first';

CREATE INDEX IF NOT EXISTS ix_online_retail_raw_overlap
    ON bronze.online_retail_raw (
        source_name, invoice_no, stock_code, quantity, invoice_date, unit_price,
        customer_id, country
    );