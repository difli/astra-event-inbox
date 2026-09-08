# Known Limitations

**astra-event-inbox** is a Proof of Concept. Specification compliance: [`specification-compliance.md`](specification-compliance.md).

---

## Open requirement clarifications (may block production)

### L-07 — Post-drain late arrivals are persisted but not automatically processed

**Classification:** Open Requirement Clarification  
**Impact:** High — **production blocker if the original requirement remains mandatory**  
**Component:** `InboxKafkaConsumer`, `InboxDrainer`  
**Traceability:** R-04, R-16

A late-arriving event — one whose `event_ts` falls in a window the drainer has already processed — is written to Astra DB in its natural bucket. The `inbox.events.late_arrival` counter is incremented and a WARN log is emitted. The event is **not** automatically forwarded to the `EventSink` unless an operator manually replays the affected window.

After `INBOX_TTL_SECONDS` (24 h), the row expires. If the event has not been replayed by then, it is gone from the inbox.

**This behaviour does not fully satisfy the source specification’s requirement that every late message must still be processed.**

Do not describe post-drain late arrivals as fully handled. Do not implement rebucketing or automatic replay until the open design questions are resolved (see [`specification-compliance.md`](specification-compliance.md) Open Design Questions 1–5).

---

### L-14 — Window sealing uses application wall-clock time

**Classification:** Open Requirement Clarification  
**Impact:** High if a stream-derived event-time watermark is mandatory  
**Component:** `InboxDrainer`  
**Traceability:** R-05

Window **assignment** is deterministic and does **not** depend on wall clock:

```
window_bucket = floor(epoch_seconds(event_ts) / W) * W
```

Window **sealing** currently does:

```
now >= window_bucket + window_size + allowed_lateness
```

The source specification proposes event-time watermark progression (a window is complete once the stream’s own timestamps have progressed past it) and says the design must not depend on synchronised clocks.

This PoC therefore:

- does **not** claim full compliance with the no-synchronised-clocks requirement
- differs from stream-derived sealing
- requires a production decision before implementation (Open Design Question 6)

Do not implement a watermark in this PoC until that decision is made.

---

### L-11 — Late events are not rebucketed at ingest

**Classification:** Intentional PoC design decision; final handling subject to clarification  
**Impact:** Coupled to L-07  
**Component:** `InboxKafkaConsumer`, `WindowBucketCalculator`  
**Traceability:** R-16

The source specification rebuckets a late event into the current open window so it is still drained. The PoC intentionally keeps deterministic natural event-time buckets and does not rebucket. `allowed_lateness_seconds` controls only the drainer’s sealed-bucket threshold, not ingest bucket assignment.

This is an intentional PoC design decision. Final late-event handling remains subject to requirements clarification.

---

## Critical operating constraints

### L-01 — Single drainer instance only (no distributed locking)

**Impact:** High  
**Component:** `InboxDrainer`  
**Traceability:** R-15

Only **one** drainer instance may run at a time. There is no distributed lock or leader election. Multiple instances can drain the same `window_bucket` more than once.

The `FileWriterEventSink` overwrites its output file on retry, so the file system output is idempotent — but any downstream system consuming those files could see duplicates.

**Workaround:** `INBOX_DRAINER_ENABLED=true` on exactly one instance. Scale writer replicas with `INBOX_DRAINER_ENABLED=false`.

---

### L-04 — File sink is a placeholder

**Impact:** High (functional)  
**Component:** `FileWriterEventSink`  
**Traceability:** R-13

The `EventSink` implementation writes JSONL files to the local filesystem. Adequate for a PoC; not a production file-delivery system:

- Files are on the JVM host disk — not shared storage
- No downstream consumer reads these files
- Duplicate, replay, and partial-output handling are not a production contract (Open Design Question 10)

---

## Significant (validation and operability)

### L-05 — No end-to-end load testing yet

**Impact:** Medium — no throughput guarantee  
**Component:** `InboxKafkaConsumer`, `EventRepository`  
**Traceability:** R-06

The required workload target is a peak of 1,000 to 2,000 messages per second with payloads of approximately 2 to 4 KB. The PoC has not yet been validated against this target. No throughput guarantee is made.

The default concurrency value of 6 is marked in `application.yml` as an "INITIAL TEST VALUE" pending load-test confirmation. The async write path (`INBOX_ASYNC_WRITES=true`) is also unvalidated.

A load-test campaign is needed using 500 msg/s as a baseline, 1,000 msg/s as an intermediate scenario, and 2,000 msg/s as the required peak scenario. An optional burst above that peak is a resilience experiment only, not a supported throughput target.

---

### L-06 — Drainer cursor loss after 7 days idle

**Impact:** Medium  
**Component:** `InboxDrainer`, `DrainProgressRepository`  
**Traceability:** R-14

The `drain_progress` table has a `default_time_to_live` of 7 days (604,800 s). If the drainer is idle for more than 7 days, progress rows expire and the cursor is lost. On the next startup the drainer re-bootstraps from `LATEST`, skipping historical windows that were not drained before TTL expiry.

The same skip applies on **first run** with no progress: `LATEST` starts at the earliest currently sealed wall-clock window and does not drain older `slup_inbox` rows.

**Workaround:** Run the drainer at least once every 7 days, or increase the `drain_progress` TTL before deploying.

---

## Production-readiness gaps (not functional correctness blockers)

Distributed tracing and a Prometheus scrape endpoint are production-readiness and operability gaps, not inbox-correctness blockers.

### L-02 — No distributed tracing

**Impact:** Operability / production-readiness  
**Component:** All

There is no OpenTelemetry or Jaeger integration. Tracing a single event from Kafka record → Astra write → drain cycle requires correlating log lines by `event_id`.

---

### L-03 — No Prometheus scrape endpoint

**Impact:** Observability / production-readiness  
**Component:** Actuator

The `micrometer-registry-prometheus` dependency is not included. `GET /actuator/metrics/<name>` exposes Micrometer metric metadata and measurements as **JSON**. That is not a Prometheus scrape target. `/actuator/prometheus` is available only after adding and configuring the Prometheus registry.

---

## Minor (low impact, accepted trade-offs)

### L-08 — Schema initializer runs on every startup

**Impact:** Low (startup latency)  
**Component:** `SchemaInitializer`

Both `schema.cql` and `schema-phase2.cql` are executed via `CREATE TABLE IF NOT EXISTS` on every application startup. Idempotent, but a small extra Astra round-trip. Production should use a dedicated migration tool.

---

### L-09 — No Kafka consumer group offset management tooling

**Impact:** Low (operability)  
**Component:** `InboxKafkaConsumer`

Replaying from Kafka requires `kafka-consumer-groups.sh` (or equivalent). There is no built-in replay UI or API.

---

### L-10 — Async write path not the default

**Impact:** Low  
**Component:** `EventRepository`

The async write path (`INBOX_ASYNC_WRITES=true`) is implemented and unit-tested but disabled by default. `saveAsync` acquires a process-wide `Semaphore(maxInFlight)` (default 4). The consumer then blocks on `CompletableFuture.allOf(...).get()` before ack. Do not enable it without load-test evidence (L-05).

---

### L-12 — Partition count hardcoded assumption

**Impact:** Low  
**Component:** `application.yml`

`KAFKA_CONSUMER_CONCURRENCY` defaults to 6, matching the expected partition count of `cloud-events`. There is no runtime check that concurrency equals partition count.

---

### L-13 — Async path still blocks the listener; hard kill can leave an unacked offset

**Impact:** Low  
**Component:** `EventRepository`, `InboxKafkaConsumer`

On the async path the listener waits for `allOf().get()` before ack. A graceful container stop waits for the current record. A **hard JVM kill** can still leave the offset uncommitted. Kafka redelivers; the idempotent upsert handles a stable-key redelivery.

---

## Summary

| ID | Category | Production blocker? |
|---|---|---|
| L-07 Post-drain late arrivals not auto-processed | Open requirement clarification | **Yes, if automatic forwarding is required** |
| L-14 Wall-clock sealing (not event-time watermark) | Open requirement clarification | Yes, if watermark sealing is mandatory |
| L-11 No ingest rebucketing | Intentional PoC deviation | Coupled to L-07 |
| L-01 Single drainer instance | Critical operating constraint | Yes (if more than one drainer replica) |
| L-04 File sink placeholder | Critical operating constraint | Yes (no production downstream) |
| L-05 No load testing | Significant | Throughput not guaranteed |
| L-06 Cursor loss after 7 days | Significant | Yes, if drainer idle past TTL |
| L-02 No distributed tracing | Production-readiness gap | No (inbox correctness unaffected) |
| L-03 No Prometheus endpoint | Production-readiness gap | No (inbox correctness unaffected) |
| L-08 Schema on startup | Minor | No |
| L-09 No replay tooling | Minor | No |
| L-10 Async path not default | Minor | No |
| L-12 Partition count assumption | Minor | No |
| L-13 Async hard-kill window | Minor | No |
