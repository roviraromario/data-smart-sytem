-- 1. Evolución mensual de las ventas netas, considerando devoluciones.
SELECT
    DATE_TRUNC('month', transaction_at AT TIME ZONE 'UTC')::DATE AS month,
    SUM(revenue_net) AS net_revenue
FROM silver.online_retail
GROUP BY 1
ORDER BY 1;

-- 2. Revenue bruto y proporción de devoluciones por categoría.
SELECT
    product_category,
    SUM(revenue_gross) FILTER (WHERE quantity > 0) AS gross_revenue,
    ABS(SUM(revenue_gross) FILTER (WHERE quantity < 0)) AS return_revenue,
    ABS(SUM(revenue_gross) FILTER (WHERE quantity < 0))
        / NULLIF(SUM(revenue_gross) FILTER (WHERE quantity > 0), 0)
        AS return_rate
FROM silver.online_retail
GROUP BY product_category
ORDER BY gross_revenue DESC NULLS LAST;

-- 3a. Diez productos con mayor revenue neto.
SELECT product_code, SUM(revenue_net) AS net_revenue
FROM silver.online_retail
GROUP BY product_code
ORDER BY net_revenue DESC
LIMIT 10;

-- 3b. Diez productos con mayor tasa de devolución entre los que tuvieron ventas.
SELECT
    product_code,
    SUM(revenue_gross) FILTER (WHERE quantity > 0) AS gross_revenue,
    ABS(SUM(revenue_gross) FILTER (WHERE quantity < 0)) AS return_revenue,
    ABS(SUM(revenue_gross) FILTER (WHERE quantity < 0))
        / NULLIF(SUM(revenue_gross) FILTER (WHERE quantity > 0), 0)
        AS return_rate
FROM silver.online_retail
GROUP BY product_code
HAVING SUM(revenue_gross) FILTER (WHERE quantity > 0) > 0
   AND SUM(revenue_gross) FILTER (WHERE quantity < 0) < 0
ORDER BY return_rate DESC NULLS LAST
LIMIT 10;

-- 4. Países con más transacciones y ticket promedio neto por factura.
WITH invoice_totals AS (
    SELECT country, invoice_no, SUM(revenue_net) AS invoice_net
    FROM silver.online_retail
    GROUP BY country, invoice_no
)
SELECT
    country,
    COUNT(*) AS invoice_count,
    AVG(invoice_net) AS average_net_ticket
FROM invoice_totals
GROUP BY country
ORDER BY invoice_count DESC;

-- 5. Comparación de transacciones de clientes identificados y anónimos.
SELECT
    CASE WHEN customer_id IS NULL THEN 'ANONIMO' ELSE 'IDENTIFICADO' END
        AS customer_type,
    COUNT(*) AS line_count,
    COUNT(DISTINCT invoice_no) AS invoice_count,
    SUM(revenue_net) AS net_revenue
FROM silver.online_retail
GROUP BY 1
ORDER BY 1;

-- 6. Productos con descripciones inconsistentes en los registros Bronze.
SELECT
    UPPER(REGEXP_REPLACE(TRIM(stock_code), '\s+', '', 'g')) AS product_code,
    COUNT(DISTINCT UPPER(REGEXP_REPLACE(TRIM(description), '\s+', ' ', 'g')))
        AS description_variants
FROM bronze.online_retail_raw
WHERE NULLIF(TRIM(stock_code), '') IS NOT NULL
  AND NULLIF(TRIM(description), '') IS NOT NULL
GROUP BY 1
HAVING COUNT(
    DISTINCT UPPER(REGEXP_REPLACE(TRIM(description), '\s+', ' ', 'g'))
) > 1
ORDER BY description_variants DESC, product_code;

-- 7. Total de códigos de producto únicos normalizados.
SELECT COUNT(DISTINCT UPPER(REGEXP_REPLACE(TRIM(stock_code), '\s+', '', 'g'))) AS unique_product_count
FROM bronze.online_retail_raw
WHERE NULLIF(TRIM(stock_code), '') IS NOT NULL;
