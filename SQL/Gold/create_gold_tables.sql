CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_product_sales (
    transaction_date DATE NOT NULL,
    product_code TEXT NOT NULL,
    product_category TEXT NOT NULL,
    country TEXT NOT NULL,
    line_count BIGINT NOT NULL,
    invoice_count BIGINT NOT NULL,
    gross_revenue NUMERIC(24, 4) NOT NULL,
    return_revenue NUMERIC(24, 4) NOT NULL,
    net_revenue NUMERIC(24, 4) NOT NULL,
    PRIMARY KEY (transaction_date, product_code, product_category, country)
);