#!/usr/bin/env bash
# =============================================================================
# start.sh — Start astra-event-inbox locally
#
# Usage:
#   ./start.sh           # normal start (sync writes, Java 21)
#   ./start.sh --async   # start with async writes enabled
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Java 21
# ---------------------------------------------------------------------------
export JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null) \
  || { echo "ERROR: Java 21 not found. Install with: brew install openjdk@21"; exit 1; }
export PATH="$JAVA_HOME/bin:$PATH"
echo "Java: $(java -version 2>&1 | head -1)"

# ---------------------------------------------------------------------------
# Load .env — clean slate first (unset all inbox vars to avoid stale values)
# ---------------------------------------------------------------------------
if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "ERROR: .env not found. Copy .env.example to .env and fill in your credentials."
  exit 1
fi

# Unset all variables that .env might define, so stale shell values cannot
# override the file. Add any new variables here if you extend .env.
unset ASTRA_SECURE_BUNDLE_PATH_HOST ASTRA_CLIENT_ID ASTRA_CLIENT_SECRET
unset ASTRA_KEYSPACE KAFKA_BOOTSTRAP_SERVERS KAFKA_CONSUMER_GROUP
unset KAFKA_TOPIC KAFKA_DLT_TOPIC KAFKA_MAX_POLL_RECORDS KAFKA_CONSUMER_CONCURRENCY
unset INBOX_WINDOW_SIZE_SECONDS INBOX_ASYNC_WRITES INBOX_MAX_IN_FLIGHT_WRITES
unset INBOX_ASTRA_ASYNC INBOX_ASTRA_MAX_IN_FLIGHT
unset ASTRA_RETRY_MAX_ATTEMPTS ASTRA_RETRY_BASE_DELAY_MS ASTRA_RETRY_MAX_DELAY_MS

set -a
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.env"
set +a

# ---------------------------------------------------------------------------
# Host-side overrides (bundle path, local Kafka)
# ---------------------------------------------------------------------------
export ASTRA_SECURE_BUNDLE_PATH="${ASTRA_SECURE_BUNDLE_PATH_HOST}"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

# ---------------------------------------------------------------------------
# Optional --async flag
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--async" ]]; then
  export INBOX_ASYNC_WRITES=true
  echo "Mode: async writes ENABLED"
else
  export INBOX_ASYNC_WRITES=false
  echo "Mode: sync writes (default)"
fi

# ---------------------------------------------------------------------------
# Print effective config summary
# ---------------------------------------------------------------------------
echo "Astra keyspace : ${ASTRA_KEYSPACE}"
echo "Kafka brokers  : ${KAFKA_BOOTSTRAP_SERVERS}"
echo "Bundle         : ${ASTRA_SECURE_BUNDLE_PATH}"
echo ""

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
exec mvn spring-boot:run
