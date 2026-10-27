#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if ! airflow users list --output json |
  python -c 'import json,sys; users=json.load(sys.stdin); print(str(any(user.get("username") == sys.argv[1] for user in users)).lower())' "$AIRFLOW_ADMIN_USERNAME" |
  grep -qx true; then
  airflow users create \
    --username "$AIRFLOW_ADMIN_USERNAME" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname "$AIRFLOW_ADMIN_FIRSTNAME" \
    --lastname "$AIRFLOW_ADMIN_LASTNAME" \
    --role Admin \
    --email "$AIRFLOW_ADMIN_EMAIL"
fi
