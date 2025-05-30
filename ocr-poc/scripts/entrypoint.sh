#!/bin/bash
set -e

# Initialize log file
LOG_FILE="/app/logs/db_init.log"
mkdir -p /app/logs
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting entrypoint script" | tee -a "$LOG_FILE"

# Validate environment variables
: "${ep_stage_db_user:?Missing ep_stage_db_user}"
: "${ep_stage_db_password:?Missing ep_stage_db_password}"
: "${ep_stage_db_host:?Missing ep_stage_db_host}"
: "${ep_stage_db_port:?Missing ep_stage_db_port}"
: "${ep_stage_db:?Missing ep_stage_db}"
export PGPASSWORD="$ep_stage_db_password"

# Check PostgreSQL connection
if psql -h "$ep_stage_db_host" -p "$ep_stage_db_port" -U "$ep_stage_db_user" -d "$ep_stage_db" -c "SELECT 1" >/dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Successfully connected to PostgreSQL at $ep_stage_db_host:$ep_stage_db_port" | tee -a "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to connect to PostgreSQL at $ep_stage_db_host:$ep_stage_db_port" >&2 | tee -a "$LOG_FILE"
    exit 1
fi

# Run init_db.sql only if INIT_DB is set to true
if [ "$INIT_DB" = "true" ] && [ -f "/docker-entrypoint-initdb.d/init_db.sql" ]; then
    if psql -h "$ep_stage_db_host" -p "$ep_stage_db_port" -U "$ep_stage_db_user" -d "$ep_stage_db" -f /docker-entrypoint-initdb.d/init_db.sql >/dev/null 2>&1; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Successfully executed init_db.sql" | tee -a "$LOG_FILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failed to execute init_db.sql" >&2 | tee -a "$LOG_FILE"
        exit 1
    fi
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Skipping init_db.sql (INIT_DB=$INIT_DB or file missing)" | tee -a "$LOG_FILE"
fi

# Log command execution
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Executing command: $@" | tee -a "$LOG_FILE"

# Start the application
exec "$@" 2>&1 | tee -a "$LOG_FILE"