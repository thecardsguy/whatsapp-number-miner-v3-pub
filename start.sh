#!/bin/sh
set -e

echo "=== WhatsApp Number Miner v3.3.3 ==="
mkdir -p /app/data

# Validate required env vars
if [ -z "${APP_PASSWORD}" ]; then
    echo "ERROR: APP_PASSWORD environment variable is required"
    exit 1
fi

# Generate GOWA_BASIC_AUTH if not set
if [ -z "${GOWA_BASIC_AUTH}" ]; then
    RANDOM_PW=$(head -c 16 /dev/urandom | base64 | tr -d '+/=' | head -c 16)
    export GOWA_BASIC_AUTH="admin:${RANDOM_PW}"
    echo "Generated random GOWA auth credentials."
fi

# Persist GOWA sessions in /app/data or Postgres DATABASE_URL
if echo "${DATABASE_URL}" | grep -q "^postgres"; then
    case "${DATABASE_URL}" in
        *sslmode=*) export GOWA_DB_URI="${DATABASE_URL}" ;;
        *\?*) export GOWA_DB_URI="${DATABASE_URL}&sslmode=require" ;;
        *) export GOWA_DB_URI="${DATABASE_URL}?sslmode=require" ;;
    esac
else
    export GOWA_DB_URI="${GOWA_DB_URI:-file:/app/data/gowa.db?_foreign_keys=on}"
fi

echo "Starting GOWA v9.8.0 on 127.0.0.1:3000..."
/app/whatsapp rest \
    --port=3000 \
    --os=Chrome \
    --basic-auth="${GOWA_BASIC_AUTH}" \
    --db-uri="${GOWA_DB_URI}" \
    --account-validation=false \
    --auto-mark-read=false \
    --auto-reject-call=true \
    --auto-download-media=false \
    --ignore-status-media=true \
    --presence-on-connect=unavailable \
    --presence-pulse-enabled=true \
    &

echo "Waiting for GOWA to become ready..."
for i in $(seq 1 30); do
    if curl -s -u "${GOWA_BASIC_AUTH}" -o /dev/null -w "%{http_code}" "http://127.0.0.1:3000/devices" 2>/dev/null | grep -q "200"; then
        echo "GOWA is ready."
        break
    fi
    sleep 1
done

echo "Starting FastAPI on :${PORT:-8000}..."
exec python -m uvicorn app:app --host 0.0.0.0 --port "${PORT:-8000}" --log-level info
