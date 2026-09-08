# Demo Guide

Step-by-step script for demonstrating `astra-event-inbox`. Estimated duration: **20–30 minutes**.

For specification compliance and known limitations, see [`requirements-traceability.md`](requirements-traceability.md) and [`production-readiness-gaps.md`](production-readiness-gaps.md).

---

## Before you start

### Two ways to run the demo

| | **Full Docker stack** | **Kafka in Docker, app on host** |
|---|---|---|
| **Start** | `docker compose up --build -d` | `docker --context colima-kafka compose -f docker-compose.kafka.yml up -d` then `./start.sh` |
| **Stop** | `docker compose down` | Ctrl+C in the `./start.sh` terminal |
| **Restart** | `docker compose restart inbox` | Ctrl+C then `./start.sh` again |
| **App logs** | `docker compose logs -f inbox` | Printed directly to the `./start.sh` terminal |
| **Drainer output** | `docker exec astra-event-inbox ls /tmp/inbox-drain/` | `ls /tmp/inbox-drain/` |
| **Enable drainer** | Set `INBOX_DRAINER_ENABLED=true` in `.env`, restart | `INBOX_DRAINER_ENABLED=true ./start.sh` |

Where commands differ between the two modes, both variants are shown with **🐳 Docker** and **💻 Host** labels.

### Setup checklist

- [ ] `.env` populated with valid Astra credentials and SCB path
- [ ] Schema applied to Astra (`schema.cql` and `schema-phase2.cql`)
- [ ] Docker running (always needed — Kafka runs in Docker in both modes)
- [ ] Terminal windows ready (recommend 3 split panes)
- [ ] Astra console open in browser (for live data verification)

### Pre-flight check

Verify the build is clean (load tests are excluded from the default run):

```bash
mvn test -q
```

Start the stack:

**🐳 Docker:**

```bash
docker compose up --build -d
```

**💻 Host:**

```bash
docker --context colima-kafka compose -f docker-compose.kafka.yml up -d
./start.sh
```

Wait for aggregate health (Astra + Kafka indicators). Note: `/actuator/health/readiness` is Spring readinessState only and does not check Astra or Kafka:

```bash
until curl -sf http://localhost:8080/actuator/health; do sleep 3; done
echo "Ready."
```

---

## Demo script

### Scene 1 — Show the service is running (2 min)

> *"The service is up. Let's look at what it exposes."*

Aggregate health — Astra and Kafka indicators are here (Compose healthcheck uses this URL):

```bash
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
```

Expected output (`show-details: always` → each component includes a `details` map; keys and values vary):

```json
{
  "status": "UP",
  "components": {
    "astra": { "status": "UP", "details": {} },
    "diskSpace": { "status": "UP", "details": {} },
    "kafkaConsumer": { "status": "UP", "details": {} },
    "livenessState": { "status": "UP" },
    "ping": { "status": "UP" },
    "readinessState": { "status": "UP" }
  }
}
```

> *" `/actuator/health` is the aggregate: Astra (`SELECT release_version FROM system.local`) and Kafka (AdminClient / consumer group) are included here. `/actuator/health/liveness` and `/actuator/health/readiness` are Spring state probes only — they do not wait for Astra or Kafka. Kubernetes should not treat this PoC's `/readiness` URL as a dependency check."*

---

### Scene 2 — Ingest a single event (3 min)

> *"Let's publish a CloudEvent and watch it land in the database."*

Open a terminal watching the metrics before publishing:

Check the success counter (should be 0 initially):

```bash
curl -s http://localhost:8080/actuator/metrics/inbox.events.success
```

Publish the event:

```bash
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EOF'
{"specversion":"1.0","id":"demo-event-001","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:00Z","datacontenttype":"application/json","data":{"guid":"000000000000000100000001","operation":"SET","requestName":"demo","applicationId":"demo","timestamp":"2026-07-16T10:00:00Z","clientAddress":"10.20.30.40","fields":[]}}
EOF
```

Check the counter again:

```bash
curl -s http://localhost:8080/actuator/metrics/inbox.events.success
```

Expected: `"value": 1.0`

> *"The counter incremented. The event was parsed, bucketed from `data.timestamp`, and written to Astra DB with `LOCAL_QUORUM`. `payload` stores the entire raw Kafka JSON. `window_bucket` for `2026-07-16T10:00:00Z` is `1784196000`."*

Verify in Astra (use cqlsh or the Astra console CQL shell):

```sql
SELECT window_bucket, event_ts, event_id, operation, ingest_time
FROM tds_inbox.slup_inbox
LIMIT 5;
```

> *"Here's the row in Cassandra. Notice `window_bucket` — it's the event timestamp floored to the nearest 5-second boundary. This is the partition key; all events in the same 5-second window share a partition."*

---

### Scene 3 — Demonstrate deduplication (3 min)

> *"Now let's publish the exact same event again — same `id`, same `time`. In a conventional inbox, this would create a duplicate. Here it doesn't."*

Publish the same event twice more:

```bash
for i in 1 2; do
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EOF'
{"specversion":"1.0","id":"demo-event-001","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:00Z","datacontenttype":"application/json","data":{"guid":"000000000000000100000001","operation":"SET","requestName":"demo","applicationId":"demo","timestamp":"2026-07-16T10:00:00Z","clientAddress":"10.20.30.40","fields":[]}}
EOF
done
```

Count rows for this event_id — full primary key, no ALLOW FILTERING needed. Result should be 1:

```sql
SELECT COUNT(*) FROM tds_inbox.slup_inbox
WHERE window_bucket = 1784196000 AND event_ts = '2026-07-16 10:00:00+0000' AND event_id = 'demo-event-001';
```

Check the success counter — it incremented by 2 more (the writes happened), but only 1 row exists:

```bash
curl -s http://localhost:8080/actuator/metrics/inbox.events.success
```

Expected: `"value": 3.0`

> *"Three messages consumed, three writes to Cassandra — but only one row. The Cassandra primary key `(window_bucket, event_ts, event_id)` makes every INSERT an idempotent upsert. No read-before-write, no deduplication table, no extra latency. Kafka redeliveries after a crash are handled the same way."*

---

### Scene 4 — Dead-letter routing for bad messages (3 min)

> *"What happens when a message arrives that can't be parsed?"*

Produce a malformed message:

```bash
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EOF'
this-is-not-a-cloud-event
EOF
```

Parse error counter — expected `"value": 1.0`:

```bash
curl -s http://localhost:8080/actuator/metrics/inbox.events.parse_error
```

Inspect the DLT:

```bash
docker exec inbox-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events.DLT \
  --from-beginning \
  --max-messages 3
```

> *"The bad message was forwarded to the dead-letter topic and the offset was committed **because the DLT send succeeded**. If the DLT send had failed, the listener would throw and the offset would not be committed. Astra write failures are retried, not sent to the DLT."*

---

### Scene 5 — Publish a batch of events across multiple windows (3 min)

> *"Let's produce events across several different time windows and see how they're bucketed."*

5 events spread across 3 different 5-second windows:

```bash
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<'EVENTS'
{"specversion":"1.0","id":"batch-w1-001","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:01Z","datacontenttype":"application/json","data":{"guid":"100000000000000000000001","operation":"SET","requestName":"demo","applicationId":"app1","timestamp":"2026-07-16T10:00:01Z","clientAddress":"10.0.0.1","fields":[]}}
{"specversion":"1.0","id":"batch-w1-002","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:03Z","datacontenttype":"application/json","data":{"guid":"100000000000000000000002","operation":"DELETE","requestName":"demo","applicationId":"app1","timestamp":"2026-07-16T10:00:03Z","clientAddress":"10.0.0.2","fields":[]}}
{"specversion":"1.0","id":"batch-w2-001","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:06Z","datacontenttype":"application/json","data":{"guid":"100000000000000000000003","operation":"SET","requestName":"demo","applicationId":"app2","timestamp":"2026-07-16T10:00:06Z","clientAddress":"10.0.0.3","fields":[]}}
{"specversion":"1.0","id":"batch-w2-002","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:08Z","datacontenttype":"application/json","data":{"guid":"100000000000000000000004","operation":"SET","requestName":"demo","applicationId":"app2","timestamp":"2026-07-16T10:00:08Z","clientAddress":"10.0.0.4","fields":[]}}
{"specversion":"1.0","id":"batch-w3-001","source":"urn:demo","type":"demo.event.v1","time":"2026-07-16T10:00:11Z","datacontenttype":"application/json","data":{"guid":"100000000000000000000005","operation":"DELETE","requestName":"demo","applicationId":"app3","timestamp":"2026-07-16T10:00:11Z","clientAddress":"10.0.0.5","fields":[]}}
EVENTS
```

Query Astra to show the window partitioning:

```sql
SELECT window_bucket, event_ts, event_id, operation
FROM tds_inbox.slup_inbox
LIMIT 20;
```

> *"Events with timestamps between 10:00:00 and 10:00:04 land in bucket 1784196000. Events between 10:00:05 and 10:00:09 land in 1784196005. Within each bucket the rows come back in event-time order — that's the clustering key doing its job."*

---

### Scene 6 — Drainer (5–8 min)

> *"The drainer reads sealed windows and writes JSONL via `FileWriterEventSink`. It is disabled by default (`INBOX_DRAINER_ENABLED=false`). `@ConditionalOnProperty` is evaluated at startup, so enabling it requires a restart."*

**Why the July 2026 events from Scenes 2–5 will not drain under the default bootstrap**

Those events have `event_ts` in July 2026. Default bootstrap is `LATEST`: the cursor is set to the earliest **currently sealed wall-clock** window, so historical inbox rows are skipped.

Do **not** set `INBOX_DRAINER_BOOTSTRAP_MODE=CONFIGURED` with `INBOX_DRAINER_START_BUCKET=1784196000` in a live demo: the drainer walks **every** sealed 5-second window from that bucket until wall-clock (including empty ones).

Produce a few events with **current UTC** timestamps **after** the drainer is already running with `LATEST`. If you produce historical timestamps first and then start the drainer, `LATEST` will skip those rows.

**Step 1.** Enable the drainer and restart the app.

> If `drain_progress` already has rows the drainer resumes from that cursor. Optional: `TRUNCATE tds_inbox.drain_progress;` for a clean run.

**🐳 Docker:** Set `INBOX_DRAINER_ENABLED=true` in `.env` (a host-shell prefix is not passed into the container), then restart:

```bash
docker compose down
docker compose up -d
until curl -sf http://localhost:8080/actuator/health; do sleep 3; done
```

**💻 Host:** Pass the flag inline — no `.env` change needed:

```bash
INBOX_DRAINER_ENABLED=true ./start.sh
```

**Step 2.** Produce events dated "now" so they land in the current open window (same command for both modes — Kafka is always in Docker):

```bash
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
docker exec -i inbox-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events <<EOF
{"specversion":"1.0","id":"drain-demo-001","source":"urn:demo","type":"demo.event.v1","time":"${NOW}","datacontenttype":"application/json","data":{"guid":"200000000000000000000001","operation":"SET","requestName":"demo","applicationId":"demo","timestamp":"${NOW}","clientAddress":"10.0.0.9","fields":[]}}
{"specversion":"1.0","id":"drain-demo-002","source":"urn:demo","type":"demo.event.v1","time":"${NOW}","datacontenttype":"application/json","data":{"guid":"200000000000000000000002","operation":"SET","requestName":"demo","applicationId":"demo","timestamp":"${NOW}","clientAddress":"10.0.0.9","fields":[]}}
EOF
```

**Step 3.** Wait for the window to seal (5 s window + 60 s lateness + poll interval):

```bash
echo "Waiting 70s for the window to seal…"
sleep 70
```

**Step 4.** Inspect the output and logs.

**🐳 Docker:**

```bash
docker exec astra-event-inbox ls -la /tmp/inbox-drain/
```

```bash
docker compose logs inbox | grep -i drain
```

**💻 Host:**

```bash
ls -la /tmp/inbox-drain/
```

Drain log lines print directly to the `./start.sh` terminal. Look for:

```
Drainer [primary] no prior progress — bootstrapMode=LATEST starting from cursor=…
Drainer [primary]: draining bucket=…
Drainer [primary]: bucket=… read N events
FileWriterEventSink: wrote N event(s) → /tmp/inbox-drain/primary_<bucket>.jsonl
Drainer [primary]: bucket=… drained and recorded (events=N)
```

Check drain progress in Astra (same for both modes):

```sql
SELECT drain_id, window_bucket, event_count, drained_at
FROM tds_inbox.drain_progress;
```

> *"Each drained window is recorded in `drain_progress`. On restart with existing progress, the cursor is that last bucket and the next read is `cursor + W`. If progress was never written, that window is retried. `LATEST` with no progress starts at the earliest currently sealed window — it does not drain historical rows and it does not claim that no window is ever skipped."*

---

### Scene 7 — Crash recovery (3 min)

> *"What happens if the service crashes mid-write?"*

> *(Explain, no need to induce an actual crash in a demo)*

> *"When Kafka delivers a record, the offset is committed after a successful Astra write, or after a successful DLT publish for an unparseable message. If the JVM crashes between a successful Astra write and the ack, Kafka redelivers; the upsert produces the same row. Astra failures are not acked and are not sent to the DLT."*

Show the test that covers this guarantee:

```bash
grep -r "offset" src/test/ --include="*.java" -l
```

---

### Scene 8 — Metrics summary (2 min)

```bash
echo "=== Inbox Metrics ==="
for m in success parse_error astra_error late_arrival; do
  val=$(curl -s http://localhost:8080/actuator/metrics/inbox.events.$m \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['measurements'][0]['value'])")
  echo "  inbox.events.$m = $val"
done
```

> *"Four counters tell the story of what the service has processed: successful writes, parse errors routed to DLT, Astra errors retried, and late arrivals. These are standard Micrometer JSON metrics at `/actuator/metrics`. `/actuator/prometheus` is not enabled (L-03). Late arrivals after drain are persisted and counted but not automatically forwarded (L-07)."*

---

## Clean up after the demo

**🐳 Docker:**

```bash
docker compose down
```

**💻 Host:** Ctrl+C in the `./start.sh` terminal, then stop Kafka:

```bash
docker --context colima-kafka compose -f docker-compose.kafka.yml down
```

Optional — wipe Astra data (same for both modes):

```bash
cqlsh -u $ASTRA_CLIENT_ID -p $ASTRA_CLIENT_SECRET \
  --secure-connect-bundle $ASTRA_SECURE_BUNDLE_PATH_HOST \
  -k tds_inbox \
  -e "TRUNCATE slup_inbox; TRUNCATE drain_progress;"
```

---

## Talking points

| Topic | Key message |
|---|---|
| **Why Cassandra for an inbox?** | Time-series write pattern (append-only, partition by time window) is a natural fit. Idempotent upsert deduplication is free. No ORM, no transactions — just prepared CQL statements. |
| **Why manual Kafka offset commit?** | At-least-once: the offset advances after a successful Astra write, or after a successful DLT send for unparseable records. Astra failures seek back and retry. Same `(event_id, event_ts)` upserts one row. |
| **Why not delete rows after draining?** | The spec proposes a partition DELETE after output. This PoC records `drain_progress` and lets rows expire via TTL. That reduces explicit deletes and retains data until TTL; storage depends on TTL and rate. TTL expiry still creates tombstones. |
| **How does it scale?** | The specification target is a peak of 1,000 to 2,000 messages per second. This PoC has not yet been performance-validated, and no throughput guarantee is made. The drainer is single-instance only (L-01). Async writes still wait on each record before ack. |
| **Late events after drain** | Events arriving after their bucket was drained are detected and persisted but are not automatically forwarded in the current PoC. This is an open requirement clarification (L-07). |
| **What's next?** | See [`future-enhancements.md`](future-enhancements.md) and [`requirements-traceability.md`](requirements-traceability.md). |
