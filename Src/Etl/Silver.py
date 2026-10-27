import csv
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from io import StringIO

from Utils.Database import postgres_cursor

DATE_FORMATS = {
    "month_first": (
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
    ),
    "day_first": (
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ),
}
ISO_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
)

CATEGORY_KEYWORDS = {
    "HOGAR": (
        "CANDLE",
        "CUP",
        "HANGER",
        "KITCHEN",
        "JAR",
        "LANTERN",
        "MUG",
        "ORNAMENT",
        "T-LIGHT",
        "VASE",
    ),
    "ROPA": (
        "COAT",
        "DRESS",
        "GLOVE",
        "HAT",
        "JACKET",
        "SCARF",
        "SHIRT",
        "SKIRT",
        "SOCK",
        "TROUSER",
    ),
    "DEPORTES": ("BICYCLE", "FOOTBALL", "GYM", "SPORT", "TENNIS"),
    "PAPELERIA": ("BOOK", "CARD", "NOTEBOOK", "PAPER", "PEN", "STATIONERY"),
    "ELECTRONICA": (
        "BATTERY",
        "CAMERA",
        "ELECTRONIC",
        "MOUSE",
        "PHONE",
        "USB",
    ),
}

CREATE_STAGING_TABLES = """
CREATE TEMP TABLE stage_online_retail_silver (
    source_name TEXT,
    source_row_id BIGINT,
    invoice_no TEXT,
    product_code TEXT,
    description TEXT,
    product_category TEXT,
    quantity INTEGER,
    transaction_at TIMESTAMPTZ,
    unit_price NUMERIC(18, 4),
    customer_id BIGINT,
    country TEXT,
    line_type TEXT,
    revenue_gross NUMERIC(24, 4),
    revenue_net NUMERIC(24, 4)
) ON COMMIT DROP;
CREATE TEMP TABLE stage_rejected_rows (
    source_name TEXT,
    source_row_id BIGINT,
    reason TEXT,
    raw_record JSONB
) ON COMMIT DROP
"""

COPY_SILVER_SQL = """
COPY stage_online_retail_silver (
    source_name, source_row_id, invoice_no, product_code, description,
    product_category, quantity, transaction_at, unit_price, customer_id,
    country, line_type, revenue_gross, revenue_net
) FROM STDIN WITH (FORMAT CSV)
"""

COPY_REJECTED_SQL = """
COPY stage_rejected_rows (source_name, source_row_id, reason, raw_record)
FROM STDIN WITH (FORMAT CSV)
"""

READ_BRONZE_SQL = """
SELECT
    source_name,
    source_row_id,
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    source_date_format,
    unit_price,
    customer_id,
    country
FROM bronze.online_retail_raw
ORDER BY
    CASE source_name WHEN 'online_retail' THEN 0 ELSE 1 END,
    source_row_id
"""

PUBLISH_SILVER_SQL = """
INSERT INTO silver.online_retail (
    source_name, source_row_id, invoice_no, product_code, description,
    product_category, quantity, transaction_at, unit_price, customer_id,
    country, line_type, revenue_gross, revenue_net
)
SELECT
    source_name, source_row_id, invoice_no, product_code, description,
    product_category, quantity, transaction_at, unit_price, customer_id,
    country, line_type, revenue_gross, revenue_net
FROM stage_online_retail_silver
"""

PUBLISH_REJECTS_SQL = """
INSERT INTO audit.rejected_rows (
    source_name, source_row_id, reason, raw_record
)
SELECT source_name, source_row_id, reason, raw_record
FROM stage_rejected_rows
"""


def _parse_datetime(value: str, date_format: str) -> datetime:
    value = value.strip()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
        for date_pattern in (*DATE_FORMATS[date_format], *ISO_DATE_FORMATS):
            try:
                parsed = datetime.strptime(value, date_pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError("fecha inválida")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_customer_id(value: str) -> int | None:
    value = value.strip()
    if not value or value.casefold() in {"na", "n/a", "null", "none"}:
        return None
    customer_id = Decimal(value)
    if (
        not customer_id.is_finite()
        or customer_id != customer_id.to_integral_value()
    ):
        raise ValueError("customer ID inválido")
    if customer_id < 0:
        raise ValueError("customer ID inválido")
    return int(customer_id)


def _normalize_product_code(value: str | None) -> str:
    return "".join((value or "").split()).upper()


def _assign_category(description: str | None) -> str:
    normalized = (description or "").upper()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            return category
    return "SIN_CATEGORIA"


def _transaction_key(
    invoice_no: str,
    product_code: str,
    quantity: int,
    transaction_at: datetime,
    unit_price: Decimal,
    customer_id: int | None,
    country: str,
) -> tuple[str, ...]:
    return (
        invoice_no.strip().upper(),
        product_code.strip().upper(),
        str(quantity),
        transaction_at.isoformat(),
        str(unit_price.normalize()),
        "" if customer_id is None else str(customer_id),
        country.strip().casefold(),
    )


def _serialize_copy_rows(copy_sql, rows, cursor) -> None:
    buffer = StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow(row)
    if buffer.tell():
        buffer.seek(0)
        cursor.copy_expert(copy_sql, buffer)


def transformar_online_retail(
    postgres_hook,
    chunk_size: int,
) -> dict[str, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que cero")

    loaded_count = 0
    rejected_count = 0
    seen_sales_keys = set()

    with postgres_cursor(postgres_hook) as cursor:
        cursor.execute(CREATE_STAGING_TABLES)
        with cursor.connection.cursor(name="silver_source") as source_cursor:
            source_cursor.execute(READ_BRONZE_SQL)

            while raw_rows := source_cursor.fetchmany(chunk_size):
                clean_rows = []
                rejected_rows = []

                for raw in raw_rows:
                    (
                        source_name,
                        source_row_id,
                        invoice_no,
                        stock_code,
                        description,
                        raw_quantity,
                        raw_invoice_date,
                        source_date_format,
                        raw_unit_price,
                        raw_customer_id,
                        country,
                    ) = raw
                    raw_record = json.dumps(
                        {
                            "invoice_no": invoice_no,
                            "stock_code": stock_code,
                            "description": description,
                            "quantity": raw_quantity,
                            "invoice_date": raw_invoice_date,
                            "unit_price": raw_unit_price,
                            "customer_id": raw_customer_id,
                            "country": country,
                        },
                        ensure_ascii=False,
                    )

                    try:
                        invoice_no = (invoice_no or "").strip()
                        product_code = _normalize_product_code(stock_code)
                        country = (country or "").strip()
                        if not invoice_no or not product_code or not country:
                            raise ValueError(
                                "factura, código de producto o país vacío"
                            )

                        quantity = int(raw_quantity)
                        unit_price = Decimal(raw_unit_price)
                        if not unit_price.is_finite() or unit_price <= 0:
                            raise ValueError(
                                "precio unitario no positivo o inválido"
                            )

                        transaction_at = _parse_datetime(
                            raw_invoice_date,
                            source_date_format,
                        )
                        customer_id = _parse_customer_id(raw_customer_id or "")
                    except (
                        InvalidOperation,
                        TypeError,
                        ValueError,
                        OverflowError,
                    ) as error:
                        rejected_rows.append(
                            (
                                source_name,
                                source_row_id,
                                str(error),
                                raw_record,
                            )
                        )
                        rejected_count += 1
                        continue

                    transaction_key = _transaction_key(
                        invoice_no,
                        product_code,
                        quantity,
                        transaction_at,
                        unit_price,
                        customer_id,
                        country,
                    )
                    # Keep the daily source when transactions overlap.
                    if source_name == "online_retail_ii":
                        if transaction_key in seen_sales_keys:
                            continue
                    else:
                        seen_sales_keys.add(transaction_key)

                    description = (
                        " ".join((description or "").split()).upper() or None
                    )
                    product_category = _assign_category(description)
                    line_type = (
                        "SALE"
                        if quantity > 0
                        else "RETURN"
                        if quantity < 0
                        else "ADJUSTMENT"
                    )
                    revenue = unit_price * quantity
                    clean_rows.append(
                        (
                            source_name,
                            source_row_id,
                            invoice_no,
                            product_code,
                            description,
                            product_category,
                            quantity,
                            transaction_at.isoformat(),
                            str(unit_price),
                            customer_id,
                            country,
                            line_type,
                            str(revenue),
                            str(revenue),
                        )
                    )
                    loaded_count += 1

                _serialize_copy_rows(COPY_SILVER_SQL, clean_rows, cursor)
                _serialize_copy_rows(COPY_REJECTED_SQL, rejected_rows, cursor)

        cursor.execute("TRUNCATE TABLE silver.online_retail")
        cursor.execute("TRUNCATE TABLE audit.rejected_rows")
        cursor.execute(PUBLISH_SILVER_SQL)
        cursor.execute(PUBLISH_REJECTS_SQL)

    return {"loaded": loaded_count, "rejected": rejected_count}
