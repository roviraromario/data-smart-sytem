from Utils.Database import postgres_cursor

PUBLISH_DAILY_PRODUCT_SALES = """
INSERT INTO gold.daily_product_sales (
    transaction_date,
    product_code,
    product_category,
    country,
    line_count,
    invoice_count,
    gross_revenue,
    return_revenue,
    net_revenue
)
SELECT
    (transaction_at AT TIME ZONE 'UTC')::DATE,
    product_code,
    product_category,
    country,
    COUNT(*),
    COUNT(DISTINCT invoice_no),
    COALESCE(SUM(revenue_gross) FILTER (WHERE quantity > 0), 0),
    COALESCE(ABS(SUM(revenue_gross) FILTER (WHERE quantity < 0)), 0),
    COALESCE(SUM(revenue_net), 0)
FROM silver.online_retail
GROUP BY
    (transaction_at AT TIME ZONE 'UTC')::DATE,
    product_code,
    product_category,
    country
"""


def cargar_ventas_diarias(postgres_hook) -> int:
    with postgres_cursor(postgres_hook) as cursor:
        cursor.execute("TRUNCATE TABLE gold.daily_product_sales")
        cursor.execute(PUBLISH_DAILY_PRODUCT_SALES)
        return cursor.rowcount
