# Local Development Guide

Day-to-day development workflow for `astra-event-inbox`.

---

## Prerequisites

| Tool | Version | Check |
|---|---|---|
| Java | 21+ | `java -version` |
| Maven | 3.9+ | `mvn -version` |
| Docker | 24+ | `docker version` |
| Git | any | `git --version` |
| `cqlsh` (optional) | any | `cqlsh --version` |

---

## First-time setup

See the [README Quick Start](../README.md#-quick-start) for a complete walkthrough. Abbreviated steps:

```bash
git clone <repo-url> && cd astra-event-inbox
cp .env.example .env          # fill in Astra credentials + SCB path
mvn test                       # verify build — should be 70 tests passing
```

---

## Two ways to run locally

### Option A — Full Docker stack (Kafka + application containerised)

**Use this when:** you want to test the containerised build or don't want to deal with environment variables on the host.

Build and start everything:

```bash
docker compose up --build
```

Background mode:

```bash
docker compose up --build -d
```

Tail logs:

```bash
docker compose logs -f inbox
```

Stop:

```bash
docker compose down
```

The app is available at `http://localhost:8080`.  
Kafka is available at `localhost:9092`.

### Option B — Kafka in Docker, application on the host (faster inner loop)

**Use this when:** you are actively changing code and want fast restarts without rebuilding the Docker image.

Terminal 1 — start Kafka only:

```bash
docker --context colima-kafka compose -f docker-compose.kafka.yml up -d
```

Terminal 2 — always use `start.sh`, not `mvn spring-boot:run` directly. `start.sh` selects Java 21, loads `.env`, and remaps `ASTRA_SECURE_BUNDLE_PATH_HOST` → `ASTRA_SECURE_BUNDLE_PATH` which `application.yml` requires:

```bash
./start.sh
```

For IntelliJ / VS Code — run `AstraEventInboxApplication` with the environment variables from `.env` set in your run configuration, and set `ASTRA_SECURE_BUNDLE_PATH=$ASTRA_SECURE_BUNDLE_PATH_HOST` manually.

---

## Running tests

All unit tests (fast, no external dependencies):

```bash
mvn test
```

Single test class:

```bash
mvn test -Dtest=InboxDrainerTest
```

Single test method:

```bash
mvn test -Dtest=InboxDrainerTest#drainBucket_writesProgressAndAdvancesCursor
```

Tests matching a pattern:

```bash
mvn test -Dtest="*Drainer*"
```

Load tests (require Kafka + Astra to be running):

```bash
mvn test -Dgroups=loadtest
```

Skip tests (build JAR only):

```bash
mvn package -DskipTests
```

---

## Checking service health

Aggregate health:

```bash
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
```

Liveness only:

```bash
curl -s http://localhost:8080/actuator/health/liveness
```

Readiness only:

```bash
curl -s http://localhost:8080/actuator/health/readiness
```

All metrics:

```bash
curl -s http://localhost:8080/actuator/metrics
```

Specific counters:

```bash
curl -s http://localhost:8080/actuator/metrics/inbox.events.success
curl -s http://localhost:8080/actuator/metrics/inbox.events.parse_error
curl -s http://localhost:8080/actuator/metrics/inbox.events.astra_error
curl -s http://localhost:8080/actuator/metrics/inbox.events.late_arrival
```

---

## Producing test events

### Single event

```bash
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EOF'
{"specversion":"1.0","id":"dev-test-001","source":"urn:dev","type":"demo.event.v1","time":"2026-07-16T10:00:00Z","datacontenttype":"application/json","data":{"guid":"000000000000000000000001","operation":"SET","requestName":"demo","applicationId":"demo","timestamp":"2026-07-16T10:00:00Z","clientAddress":"127.0.0.1","fields":[]}}
EOF
```

### Batch of events (10 unique IDs)

```bash
for i in $(seq 0 9); do
  sec=$(printf '%02d' "$i")
  echo "{\"specversion\":\"1.0\",\"id\":\"dev-batch-$(printf '%03d' "$i")\",\"source\":\"urn:dev\",\"type\":\"demo.event.v1\",\"time\":\"2026-07-16T10:00:${sec}Z\",\"datacontenttype\":\"application/json\",\"data\":{\"guid\":\"00000000000000000000000${i}\",\"operation\":\"SET\",\"requestName\":\"demo\",\"applicationId\":\"demo\",\"timestamp\":\"2026-07-16T10:00:${sec}Z\",\"clientAddress\":\"127.0.0.1\",\"fields\":[]}}"
done | docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events
```

Timestamps are valid ISO-8601 (`2026-07-16T10:00:00Z` … `2026-07-16T10:00:09Z`).

### Intentionally malformed event (tests DLT routing)

```bash
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EOF'
not-valid-json-at-all
EOF
```

Verify it landed in the DLT:

```bash
docker exec inbox-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events.DLT \
  --from-beginning \
  --max-messages 5
```

---

## Querying Astra DB

Open a cqlsh session:

```bash
cqlsh -u $ASTRA_CLIENT_ID -p $ASTRA_CLIENT_SECRET \
  --secure-connect-bundle $ASTRA_SECURE_BUNDLE_PATH_HOST \
  -k tds_inbox
```

Within cqlsh:

```sql
SELECT window_bucket, event_ts, event_id, operation FROM slup_inbox LIMIT 20;
SELECT window_bucket, event_ts, event_id FROM slup_inbox WHERE window_bucket = <bucket>;
SELECT drain_id, window_bucket, event_count, drained_at FROM drain_progress;
DESCRIBE TABLE slup_inbox;
DESCRIBE TABLE drain_progress;
```

---

## Working with the drainer

The drainer is **disabled by default** (`INBOX_DRAINER_ENABLED=false`). To enable it in your local run:

Option A — via start.sh (recommended):

```bash
INBOX_DRAINER_ENABLED=true INBOX_DRAINER_OUTPUT_PATH=/tmp/inbox-drain ./start.sh
```

Option B — via `-D` flags:

```bash
INBOX_DRAINER_ENABLED=true mvn spring-boot:run -Dinbox.drainer.output-path=/tmp/inbox-drain
```

After the drainer runs, inspect the output:

```bash
ls -la /tmp/inbox-drain/
cat /tmp/inbox-drain/primary_<bucket>.jsonl
```

To force a drain of a specific historical bucket, use `CONFIGURED` bootstrap mode. Note: the drainer then walks **every** sealed 5 s window from that start bucket until wall-clock (including empty ones). Do not use a far-past start bucket in a casual local run.

Start from epoch-second bucket 1784196000 (example: 2026-07-16T10:00:00Z):

```bash
INBOX_DRAINER_ENABLED=true \
INBOX_DRAINER_BOOTSTRAP_MODE=CONFIGURED \
INBOX_DRAINER_START_BUCKET=1784196000 \
./start.sh
```

---

## Kafka management commands

List topics:

```bash
docker exec inbox-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Describe cloud-events topic (partitions, offsets):

```bash
docker exec inbox-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic cloud-events
```

Check consumer group lag:

```bash
docker exec inbox-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group astra-event-inbox
```

Reset consumer group offset to beginning (for replay):

```bash
docker exec inbox-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group astra-event-inbox \
  --reset-offsets --to-earliest \
  --topic cloud-events \
  --execute
```

Consume from DLT:

```bash
docker exec inbox-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events.DLT \
  --from-beginning
```

---

## Rebuilding without cache

Rebuild the Docker image from scratch:

```bash
docker compose build --no-cache inbox
```

Clean Maven build:

```bash
mvn clean package -DskipTests
```

---

## Environment variables quick reference

Common variables you will change during local development:

Switch to async writes for performance testing:

```bash
export INBOX_ASYNC_WRITES=true
export INBOX_MAX_IN_FLIGHT_WRITES=8
```

Tune the window size (must restart to take effect):

```bash
export INBOX_WINDOW_SIZE_SECONDS=10
```

Enable drainer with verbose logging:

```bash
export INBOX_DRAINER_ENABLED=true
export INBOX_DRAINER_POLL_INTERVAL_MS=2000
export LOGGING_LEVEL_COM_TDS_INBOX=DEBUG
```

Simulate a faster retry for testing error paths:

```bash
export INBOX_KAFKA_RETRY_INTERVAL_MS=100
```

---

## IDE setup

### IntelliJ IDEA

1. Open project as Maven project.
2. Set Project SDK to Java 21.
3. Create a Run Configuration for `AstraEventInboxApplication`:
   - Environment variables: copy values from `.env` and add:
     - `ASTRA_SECURE_BUNDLE_PATH=/your/local/path/to/bundle.zip`
     - `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`

### VS Code

Install the **Extension Pack for Java** and **Spring Boot Extension Pack**. Create `.vscode/launch.json`:

```json
{
  "configurations": [
    {
      "type": "java",
      "name": "AstraEventInboxApplication",
      "request": "launch",
      "mainClass": "com.tds.inbox.AstraEventInboxApplication",
      "envFile": "${workspaceFolder}/.env",
      "env": {
        "ASTRA_SECURE_BUNDLE_PATH": "/your/local/path/to/bundle.zip",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
      }
    }
  ]
}
```

---

## Troubleshooting

| Problem | Diagnosis | Fix |
|---|---|---|
| App fails to start with `DriverTimeoutException` | Astra credentials or bundle path wrong | `cqlsh` test with same credentials |
| `KafkaListenerEndpointRegistry` exception on startup | Kafka not reachable | Start Kafka first: `docker compose -f docker-compose.kafka.yml up -d` |
| Events produced but not appearing in Astra | Check `inbox.events.astra_error` counter | View app logs: `docker compose logs -f inbox` |
| DLT consumer sees no messages | No parse errors have occurred | Produce a malformed event to trigger DLT routing |
| `inbox.events.late_arrival` non-zero | Events arriving after their bucket was drained | Expected PoC behaviour: persisted and counted, not automatically forwarded (L-07). See [`requirements-traceability.md`](requirements-traceability.md) R-04 |
| Drainer not producing files | `INBOX_DRAINER_ENABLED=false` (default) | Set `INBOX_DRAINER_ENABLED=true` |
| Port 9092 already in use | Another Kafka or process using the port | `lsof -i :9092` and kill the conflicting process |
