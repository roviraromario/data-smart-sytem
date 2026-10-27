#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if ! airflow users list --output json \
  | python -c 'import json,sys; users=json.load(sys.stdin); print(str(any(user.get("username") == sys.argv[1] for user in users)).lower())' "$AIRFLOW_ADMIN_USERNAME" \
  | grep -qx true; then
  airflow users create \
    --username "$AIRFLOW_ADMIN_USERNAME" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname "$AIRFLOW_ADMIN_FIRSTNAME" \
    --lastname "$AIRFLOW_ADMIN_LASTNAME" \
    --role Admin \
    --email "$AIRFLOW_ADMIN_EMAIL"
fi

# Configure the analytical warehouse connection automatically.
airflow connections add warehouse_db \
  --conn-type postgres \
  --conn-host warehouse-db \
  --conn-schema "$WAREHOUSE_DB_NAME" \
  --conn-login "$WAREHOUSE_DB_USER" \
  --conn-password "$WAREHOUSE_DB_PASSWORD" \
  --conn-port 5432 \
  || airflow connections update warehouse_db \
  --conn-type postgres \
  --conn-host warehouse-db \
  --conn-schema "$WAREHOUSE_DB_NAME" \
  --conn-login "$WAREHOUSE_DB_USER" \
  --conn-password "$WAREHOUSE_DB_PASSWORD" \
  --conn-port 5432

# Initialize operational variables used by the DAG.
airflow variables set online_retail_csv_path "/opt/airflow/data/data.csv"
airflow variables set online_retail_ii_csv_path "/opt/airflow/data/Sales Transaction v.4a.csv"
airflow variables set etl_chunk_size "${ETL_CHUNK_SIZE:-50000}"
airflow variables set max_rejection_rate "${MAX_REJECTION_RATE:-0.05}"
