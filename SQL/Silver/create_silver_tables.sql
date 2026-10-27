CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS silver.online_retail (
    source_name TEXT NOT NULL,
    source_row_id BIGINT NOT NULL,
    invoice_no TEXT NOT NULL,
    product_code TEXT NOT NULL,
    description TEXT,
    product_category TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    transaction_at TIMESTAMPTZ NOT NULL,
    unit_price NUMERIC(18, 4) NOT NULL CHECK (unit_price > 0),
    customer_id BIGINT,
    country TEXT NOT NULL,
    line_type TEXT NOT NULL CHECK (
        line_type IN ('SALE', 'RETURN', 'ADJUSTMENT')
    ),
    revenue_gross NUMERIC(24, 4) NOT NULL,
    revenue_net NUMERIC(24, 4) NOT NULL,
    PRIMARY KEY (source_name, source_row_id)
);

CREATE INDEX IF NOT EXISTS ix_online_retail_silver_product_date
    ON silver.online_retail (product_code, transaction_at);

CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    source_name TEXT NOT NULL,
    source_row_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    raw_record JSONB NOT NULL,
    PRIMARY KEY (source_name, source_row_id)
);