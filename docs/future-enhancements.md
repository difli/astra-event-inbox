# Suggested Future Enhancements

Candidate enhancements to evolve `astra-event-inbox` from a Proof of Concept toward production. Open design questions: [`requirements-traceability.md`](requirements-traceability.md). Known limitations: [`production-readiness-gaps.md`](production-readiness-gaps.md).

Do not implement late-event rebucketing or a watermark sealer before the corresponding design questions are resolved. Do not treat any item here as a decided solution.

Items are grouped by theme. Priorities are engineering suggestions.

---

## Theme 1 — Production operating constraints

These items address operating constraints that block a multi-replica or production downstream deployment. Observability gaps (Prometheus, tracing) are listed separately as production-readiness work, not inbox-correctness blockers.

---

### E-01 — Distributed drainer lock (resolves L-01)

**Priority:** P0  
**Effort:** Medium

The drainer must acquire an exclusive lock before starting a drain cycle to prevent duplicate processing when multiple service instances are running.

**Recommended approach:** Use Astra DB as the lock store with a lightweight-transaction (LWT) write:

```sql
INSERT INTO drain_lock (drain_id, owner, acquired_at)
VALUES ('primary', 'instance-uuid', toTimestamp(now()))
IF NOT EXISTS
USING TTL 30;
```

The lock TTL of 30 s (slightly longer than the drain poll interval) ensures automatic release if the lock holder crashes. Renew the lock before each drain cycle.

**Alternative:** Use a dedicated coordination service (ZooKeeper, etcd, Consul) or a managed lock service (AWS DynamoDB with conditional writes, GCP Spanner).

---

### E-02 — Prometheus metrics endpoint (addresses L-03)

**Priority:** P2 (production-readiness / observability; not an inbox-correctness blocker)  
**Effort:** Low

Add the Micrometer Prometheus registry and expose `/actuator/prometheus`. `GET /actuator/metrics` remains JSON metadata/measurements and is not a Prometheus scrape target.

```xml
<!-- pom.xml -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```yaml
# application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

Provide a Grafana dashboard definition (JSON) with panels for:
- Event throughput (success rate, parse error rate, Astra error rate)
- Late arrival rate
- Kafka consumer lag per partition
- Astra write latency p50/p95/p99 (requires adding a `Timer` in `EventRepository`)
- JVM heap and GC metrics

---

### E-03 — Real downstream sink (resolves L-04)

**Priority:** P0  
**Effort:** Medium–High (depends on target)

Replace `FileWriterEventSink` with a production sink. The `EventSink` interface is already clean — only the implementation needs to change.

**Candidate implementations:**

| Target | Implementation notes |
|---|---|
| Kafka topic | `KafkaTemplate.send()` per event; use `ProducerRecord` with `window_bucket` as key for downstream partitioning |
| REST API / webhook | `WebClient` with retry; consider batching events per window |
| Cloud storage (S3, GCS) | Write JSONL to a temp file, upload atomically; use bucket/object name convention `<drain_id>/<date>/<window_bucket>.jsonl` |
| Another database | Use the existing `CqlSession` or add a new `JdbcTemplate` |

Regardless of target, the sink must be **idempotent** — the drainer may call `write(bucket, events)` more than once for the same bucket if progress recording fails after sink write.

---

### E-04 — OpenTelemetry distributed tracing (addresses L-02)

**Priority:** P2 (production-readiness / operability; not an inbox-correctness blocker)  
**Effort:** Medium

Add `spring-boot-starter-opentelemetry` and instrument the critical path:

- Kafka consume → `CloudEventParser.parse()` → `EventRepository.save()`
- `InboxDrainer.drainBucket()` → `EventSink.write()`

Use the CloudEvent `id` as the trace/span parent where possible to correlate the full event journey from upstream producer to downstream sink.

Configure the OTLP exporter via environment variable:
```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

---

## Theme 2 — Operational hardening

---

### E-05 — Kubernetes deployment manifests (resolves deployment gap)

**Priority:** P1  
**Effort:** Medium

Provide production-ready Kubernetes manifests or a Helm chart:

```
helm/
├── Chart.yaml
├── values.yaml           ← defaults; override per environment
├── templates/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   ├── secret.yaml       ← Astra credentials + SCB (sealed with Sealed Secrets or ESO)
│   └── hpa.yaml          ← HorizontalPodAutoscaler (writer replicas only)
```

Key manifest requirements:
- `livenessProbe` → `/actuator/health/liveness` (Spring liveness state)
- For Astra/Kafka dependency checks, probe `/actuator/health` **or** configure an explicit readiness group that includes those indicators. `/actuator/health/readiness` is Spring `readinessState` only in the current PoC and does **not** include Astra or Kafka.
- `replicas: 1` enforced when `INBOX_DRAINER_ENABLED=true` (via `maxReplicas: 1` in HPA, or a separate drainer `Deployment`)
- `PodDisruptionBudget` to prevent concurrent eviction of all consumer replicas
- Separate `Deployment` for the drainer role (`replicas: 1` always) vs writer role (`replicas: N`)

---

### E-06 — Graceful shutdown for async writes (resolves L-13)

**Priority:** P1  
**Effort:** Low

Register a `SmartLifecycle` or `@PreDestroy` hook that:
1. Stops the Kafka listener container (no new messages accepted).
2. Waits for all in-flight `CompletableFuture`s in `inFlightByPartition` to complete (with a configurable timeout).
3. Commits any uncommitted offsets.
4. Closes the Kafka consumer.

This eliminates the theoretical data-ordering gap between async write dispatch and JVM shutdown.

---

### E-07 — Schema migration management (resolves L-08)

**Priority:** P2  
**Effort:** Low

Replace `SchemaInitializer` with a proper migration tool:

- **Liquibase** with the Cassandra extension (`liquibase-cassandra`)
- **Flyway** (CQL support via community plugin)
- **Astra CLI** `astra db cqlsh` in a CI pipeline step

Benefits: versioned migrations, rollback scripts, migration history table, no DDL at runtime.

---

### E-08 — Configurable `drain_progress` TTL (resolves L-06)

**Priority:** P2  
**Effort:** Low

Allow `drain_progress` TTL to be configured via `INBOX_DRAIN_PROGRESS_TTL_SECONDS` (default 604,800 = 7 days). High-availability deployments should set this to at least `2 × planned_max_drainer_idle_seconds`.

Also: add a startup check that logs a warning if the `drain_progress` TTL is less than twice the `slup_inbox` TTL — a misconfiguration that could cause the cursor to expire before the events it protects.

---

### E-09 — Replay API (resolves L-09)

**Priority:** P2  
**Effort:** Medium

Expose an operator API endpoint to trigger a selective replay:

```
POST /admin/replay
{
  "drainId": "primary",
  "fromBucket": 1784196000,
  "toBucket": 1784196600
}
```

The endpoint resets the drainer cursor to `fromBucket - W` and triggers a synchronous drain of the specified window range. Secured behind a management auth layer.

---

## Theme 3 — Performance and throughput

---

### E-10 — Load testing and async write validation (addresses L-05)

**Priority:** P1  
**Effort:** Medium

The target workload is a typical rate of hundreds of messages per second and a peak rate of 1,000 to 2,000 messages per second, with payloads of approximately 2 to 4 KB. The PoC has not been performance-validated against this target. No throughput guarantee is made.

Planned scenarios (durations and Astra credit budget must be configurable; do not treat pass criteria as a capacity claim before execution):

| Scenario | Role | Suggested rate | Notes |
|---|---|---|---|
| Baseline | Baseline | 500 msg/s | First evidence of lag, errors, retries, latency |
| Intermediate | Intermediate | 1,000 msg/s | Between typical and peak |
| Required peak | Required peak | 2,000 msg/s | Specification peak; still not a guarantee until measured |
| Optional above-peak burst | Resilience experiment only | Short burst above 2,000 msg/s | **Not** a supported throughput target. Do not cite it as application capacity |

Results must report throughput, Kafka lag, errors, retries, and latency. No numerical capacity claim is made before execution.

Use results to:
1. Confirm or revise `KAFKA_CONSUMER_CONCURRENCY`.
2. Decide whether async writes are warranted.
3. Tune `INBOX_MAX_IN_FLIGHT_WRITES` if async is enabled.

---

### E-11 — Astra write latency histogram

**Priority:** P2  
**Effort:** Low

Add a `Timer` (Micrometer) in `EventRepository` around the CQL execute call:

```java
Timer.Sample sample = Timer.start(meterRegistry);
retryPolicy.executeWithRetry(() -> session.execute(bound), event.eventId());
sample.stop(meterRegistry.timer("inbox.astra.write.latency"));
```

This enables p50/p95/p99 latency dashboards without custom instrumentation and provides the data needed to tune `maxInFlight` on the async path.

---

### E-12 — Consumer concurrency auto-tuning

**Priority:** P3  
**Effort:** Low

At startup, query Kafka's AdminClient to read the actual partition count for `KAFKA_TOPIC` and set `concurrency` dynamically to match. This prevents the mismatch described in L-12 and eliminates the need to set `KAFKA_CONSUMER_CONCURRENCY` manually when the topic is repartitioned.

---

## Theme 4 — Correctness and data quality

---

### E-13 — Dead-letter message enrichment

**Priority:** P2  
**Effort:** Low

When a message is forwarded to the DLT, enrich it with headers describing the failure:

```
X-Inbox-Error-Type: PARSE_FAILURE
X-Inbox-Error-Message: <exception message>
X-Inbox-Error-Timestamp: <ISO-8601>
X-Inbox-Original-Topic: cloud-events
X-Inbox-Original-Partition: <n>
X-Inbox-Original-Offset: <n>
```

This makes the DLT self-describing and simplifies automated DLT processors and alerting pipelines.

---

### E-16 — Confirm late-event processing semantics (prerequisite for E-14)

**Priority:** P0 (requirements)  
**Effort:** Low (design review; no code)

**Prerequisite for any late-event replay, rebucketing, or LateEventSink implementation.**

Resolve Open Design Questions 1–5 in [`requirements-traceability.md`](requirements-traceability.md): maximum lateness, whether post-drain events must be forwarded automatically, whether out-of-order emit is acceptable, whether manual replay is enough, and whether natural-bucket assignment is preferred over rebucketing.

Do not implement E-14 until this decision exists.

---

### E-14 — Late-event processing options (addresses L-07, R-04; do not treat as decided)

**Priority:** P3 until E-16 is complete  
**Effort:** Medium–High depending on option

The current PoC persists post-drain late arrivals, counts them, and logs WARN, but does not automatically forward them. That does not fully satisfy the source requirement that every late message must still be processed.

**Do not prescribe a single design.** After E-16, evaluate at least these alternatives and their trade-offs:

| Option | Idea | Trade-offs |
|---|---|---|
| Dedicated idempotent `LateEventSink` | Forward late rows on a separate path without rewriting history of already-drained files | Extra sink contract; still need idempotency and naming |
| Automated replay of affected buckets | Re-read the natural bucket and re-publish | Duplicate output unless the sink is idempotent; may reorder vs original drain |
| Separate late-event Kafka topic | Publish late events for a downstream consumer | New topic contract; inbox is no longer the only emitter |
| Original rebucketing strategy | Place late events in the current open window (source PDF) | Changes storage identity; same `event_id`+`event_ts` can land in a different `window_bucket` than on-time peers; complicates dedup |
| Longer `allowed_lateness` only | Increase the seal delay | Sufficient **only** if lateness is bounded and known (Open Design Question 1). Does not help events that still arrive after seal |

No option is selected in this PoC.

---

### E-17 — Event-time watermark evaluation (addresses L-14, R-05)

**Priority:** P2 after Open Design Question 6  
**Effort:** Medium to evaluate; do not implement in this pass

Evaluate a stream-derived event-time watermark versus current wall-clock sealing. The evaluation must cover:

- maximum observed `event_ts` watermark
- allowed lateness
- idle-stream behaviour
- durable watermark recovery after restart
- multiple Kafka partition behaviour
- clock independence (no assumption of synchronised producer/consumer clocks)

Do not implement a watermark as part of the current PoC.

---

### E-15 — Upstream event clock skew detection

**Priority:** P3  
**Effort:** Low

Log a WARNING when `abs(event_ts - ingest_time) > threshold` (configurable, default 60 s). A large skew between the event timestamp and the ingestion wall-clock time indicates a misconfigured upstream producer clock or a Kafka partition that was offline for an extended period. This is distinct from the post-drain late arrival check (L-07) and catches the upstream problem earlier.

---

## Roadmap summary

| Enhancement | Priority | Effort | Addresses |
|---|---|---|---|
| E-16 Confirm late-event processing semantics | P0 (requirements) | Low | L-07, R-04; **prerequisite for E-14** |
| E-01 Distributed drainer lock | P0 | Medium | L-01 |
| E-03 Real downstream sink | P0 | Medium–High | L-04 |
| E-10 Load testing | P1 | Medium | L-05 |
| E-05 Kubernetes / Helm | P1 | Medium | deployment gap |
| E-06 Graceful async shutdown | P1 | Low | L-13 |
| E-02 Prometheus endpoint | P2 | Low | L-03 (readiness gap, not inbox blocker) |
| E-04 OpenTelemetry tracing | P2 | Medium | L-02 (readiness gap, not inbox blocker) |
| E-17 Event-time watermark evaluation | P2 | Medium | L-14, R-05 |
| E-07 Schema migration tool | P2 | Low | L-08 |
| E-08 Configurable drain_progress TTL | P2 | Low | L-06 |
| E-09 Replay API | P2 | Medium | L-09 |
| E-11 Write latency histogram | P2 | Low | — |
| E-13 DLT message enrichment | P2 | Low | — |
| E-12 Concurrency auto-tuning | P3 | Low | L-12 |
| E-14 Late-event processing options | P3 after E-16 | Medium–High | L-07; **not a decided design** |
| E-15 Clock skew detection | P3 | Low | — |
