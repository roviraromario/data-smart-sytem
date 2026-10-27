import codecs
import csv
from collections.abc import Sequence
from io import StringIO
from pathlib import Path

from Utils.Database import postgres_cursor

SOURCE_COLUMNS = {
    "invoice_no": ("invoiceno", "invoice", "transactionno"),
    "stock_code": ("stockcode", "productno"),
    "description": ("description", "productname"),
    "quantity": ("quantity",),
    "invoice_date": ("invoicedate", "date"),
    "unit_price": ("unitprice", "price"),
    "customer_id": ("customerid", "customerno"),
    "country": ("country",),
}

ENSURE_DATE_FORMAT_COLUMN = """
ALTER TABLE bronze.online_retail_raw
ADD COLUMN IF NOT EXISTS source_date_format TEXT NOT NULL
DEFAULT 'month_first'
"""

CREATE_STAGING_TABLE = """
CREATE TEMP TABLE stage_online_retail (
    source_name TEXT,
    source_row_id BIGINT,
    invoice_no TEXT,
    stock_code TEXT,
    description TEXT,
    quantity TEXT,
    invoice_date TEXT,
    unit_price TEXT,
    customer_id TEXT,
    country TEXT,
    source_date_format TEXT
) ON COMMIT DROP
"""

COPY_STAGING_SQL = """
COPY stage_online_retail FROM STDIN WITH (FORMAT CSV)
"""

INSERT_BRONZE_SQL = """
INSERT INTO bronze.online_retail_raw (
    source_name, source_row_id, invoice_no, stock_code, description,
    quantity, invoice_date, unit_price, customer_id, country,
    source_date_format
)
SELECT
    source_name, source_row_id, invoice_no, stock_code, description,
    quantity, invoice_date, unit_price, customer_id, country,
    source_date_format
FROM stage_online_retail
"""


def _csv_encoding(csv_path: Path) -> str:
    decoder = codecs.getincrementaldecoder("utf-8-sig")()
    try:
        with csv_path.open("rb") as source_file:
            while True:
                chunk = source_file.read(65_536)
                if not chunk:
                    break
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "cp1252"


def _column_mapping(fieldnames: Sequence[str]) -> dict[str, str]:
    normalized = {
        name.strip().casefold().replace(" ", ""): name
        for name in fieldnames
    }
    mapping = {}
    missing = []

    for destination, aliases in SOURCE_COLUMNS.items():
        column = next(
            (normalized[alias] for alias in aliases if alias in normalized),
            None,
        )
        if column is None:
            missing.append(destination)
        else:
            mapping[destination] = column

    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")
    return mapping


def extraer_csv_a_bronce(
    postgres_hook,
    csv_path: str,
    chunk_size: int,
    source_name: str,
) -> int:
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que cero")
    if source_name not in {"online_retail", "online_retail_ii"}:
        raise ValueError(f"Fuente no reconocida: {source_name}")

    source_path = Path(csv_path)
    rows_loaded = 0

    with postgres_cursor(postgres_hook) as cursor:
        cursor.execute(ENSURE_DATE_FORMAT_COLUMN)
        cursor.execute(CREATE_STAGING_TABLE)

        with source_path.open(
            "r",
            encoding=_csv_encoding(source_path),
            newline="",
        ) as source_file:
            reader = csv.DictReader(source_file)
            if not reader.fieldnames:
                raise ValueError(f"El CSV no tiene encabezados: {csv_path}")

            columns = _column_mapping(reader.fieldnames)
            invoice_column = columns["invoice_no"].strip().casefold()
            date_format = (
                "month_first"
                if invoice_column in {"invoiceno", "transactionno"}
                else "day_first"
            )
            index_column = next(
                (
                    column
                    for column in reader.fieldnames
                    if column.strip().casefold() == "index"
                ),
                None,
            )

            buffer = StringIO()
            writer = csv.writer(buffer, lineterminator="\n")
            rows_in_chunk = 0

            for row_number, row in enumerate(reader, start=1):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(
                        "Número de columnas incorrecto en fila "
                        f"{row_number + 1}"
                    )

                source_row_id = row_number
                if index_column and row[index_column].strip().isdigit():
                    source_row_id = int(row[index_column].strip()) + 1

                writer.writerow(
                    (
                        source_name,
                        source_row_id,
                        *(row[columns[column]] for column in SOURCE_COLUMNS),
                        date_format,
                    )
                )
                rows_loaded += 1
                rows_in_chunk += 1

                if rows_in_chunk == chunk_size:
                    buffer.seek(0)
                    cursor.copy_expert(COPY_STAGING_SQL, buffer)
                    buffer = StringIO()
                    writer = csv.writer(buffer, lineterminator="\n")
                    rows_in_chunk = 0

            if rows_in_chunk:
                buffer.seek(0)
                cursor.copy_expert(COPY_STAGING_SQL, buffer)

        cursor.execute(
            "DELETE FROM bronze.online_retail_raw WHERE source_name = %s",
            (source_name,),
        )
        cursor.execute(INSERT_BRONZE_SQL)

    return rows_loaded
