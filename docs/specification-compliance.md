# Requirements Traceability

## Purpose

This document maps the **Event Inbox** specification requirements to the current **astra-event-inbox** Proof of Concept implementation.

It is the **authoritative source for specification compliance**. Implementation architecture is in [`architecture-engineering.md`](architecture-engineering.md). Known limitations are in [`known-limitations.md`](known-limitations.md).

**Status values:**

| Status | Meaning |
|---|---|
| **Met** | Current code satisfies the requirement as stated |
| **Partially Met** | Core behaviour exists, but the semantic contract is narrower than the specification |
| **Open Requirement Clarification** | Behaviour is documented; a product owner decision is required before production |
| **Not Yet Validated** | Requirement is stated; the PoC has not been measured against it |
| **Intentional Astra DB Adaptation** | The PoC differs from the self-managed Cassandra / HTTP-broker proposal by design |

> This PoC implements the core patterns. It is not a complete implementation of every semantic requirement. Do not treat it as full specification compliance.

---

## Executive Summary

The PoC implements the core durable-ingestion, stable-redelivery, window-ordering, and drain-progress patterns. It is not a complete implementation of every semantic requirement. Post-drain late-event processing, clock-independent event-time sealing, and target throughput remain open or unvalidated.

- Do not implement late-event rebucketing before the open design questions are resolved.
- Do not claim throughput support before validation.
- Do not claim full specification compliance.

---

## Traceability Matrix

| ID | Source requirement or design point | Status | Current implementation | Gap or deviation | Decision needed |
|---|---|---|---|---|---|
| **R-01** | Persist durably, then acknowledge. Each received message must be stored durably before it is acked. | **Met** for parseable Kafka records | Parseable records: Kafka offset is acknowledged only after a successful Astra DB write (`LOCAL_QUORUM` INSERT). Astra write failures throw; `DefaultErrorHandler` seeks back and retries. Unparseable records follow a separate DLT path and are acknowledged only after a successful DLT publication. DLT publication failure throws, seeks back, and is retried. | The source specification describes an HTTP ack. The PoC acknowledges a Kafka offset — semantic equivalent for the Kafka transport. | Confirm Kafka is the agreed transport. |
| **R-02** | Deduplicate. A message delivered more than once must be processed once. Dedup key: `event_id`, stable across redeliveries. | **Partially Met** | Storage is an idempotent CQL upsert on `PRIMARY KEY ((window_bucket), event_ts, event_id)`. Redelivery of the same `event_id` at the same `event_ts` writes the same row. | Not unconditional deduplication by `event_id` alone. The same `event_id` with a different `event_ts` produces a second row. This is at-least-once delivery plus idempotent storage for a **stable-key** redelivery. | Confirm whether production dedup is the composite key or `event_id` alone. |
| **R-03** | Reorder within a short window by `event_ts` before emit. | **Met** within each window bucket | Rows are clustered `event_ts ASC, event_id ASC`. A sealed-window `SELECT ... WHERE window_bucket = :b` returns that order. | No global ordering across all windows. Cross-window order depends on drain sequence (cursor + W). | Confirm that cross-window order is drain-order, not a single global sort. |
| **R-04** | Never drop late messages. Late messages must still be processed; out of global order is acceptable, dropping is not. | **Open Requirement Clarification** | Events arriving **before** their bucket is drained are included in the later drain. Events arriving **after** their bucket has already been drained are written durably, counted by `inbox.events.late_arrival`, logged with WARN, but **not** automatically forwarded. They expire via TTL unless manually replayed. | Post-drain late arrivals are not automatically forwarded. | See Open Questions 1–5. |
| **R-05** | Do not depend on synchronized clocks. Sealing by event-time watermark: a window is complete once the stream's own timestamps have progressed past it. | **Open Requirement Clarification** | Bucket **assignment** is deterministic and uses `event_ts` only. Window **sealing** uses the application wall clock: `now >= window_bucket + window_size + allowed_lateness`. | Assignment does not depend on wall clock. Sealing does. This differs from stream-derived watermark progression. | See Open Design Question 6. |
| **R-06** | Peak 1,000–2,000 msg/s; typical hundreds/s; ~2–4 KB payload. | **Not Yet Validated** | The PoC has not been performance-validated against this target. No throughput guarantee is made. | No end-to-end load-test evidence. Six consumer threads do not prove peak capacity. | See Open Questions 7–8. |
| **R-07** | CQL usage — one prepared INSERT per message at `LOCAL_QUORUM`, then ack. | **Met** | Java driver CQL only. No Astra Data API. Prepared INSERT, `LOCAL_QUORUM`, `setIdempotent(true)`. | None. | None. |
| **R-08** | Prepared and idempotent writes; dedup from the primary key. | **Met** under the documented stable-key contract | Prepared idempotent INSERT. Same `(window_bucket, event_ts, event_id)` upserts. | Same gap as R-02 if `event_ts` is unstable across redeliveries. | Same as R-02. |
| **R-09** | Ordered single-partition bucket reads: `SELECT ... WHERE window_bucket = :b`. | **Met** | `DrainProgressRepository.readEvents` selects named columns for one `window_bucket`, clustering order, page size 500. | Result is collected into a list (PoC heap bound), not streamed to the sink. | Confirm production sink may stream rather than materialise the full window. |
| **R-10** | Input via HTTP event broker; ack the broker. | **Intentional Astra DB Adaptation** | The PoC consumes CloudEvents 1.0 from Kafka (`cloud-events`). Ack is a Kafka offset commit. | Not a literal implementation of the HTTP input variant. | Confirm Kafka is the agreed transport. |
| **R-11** | After output is durably written, delete the drained partition. | **Intentional Astra DB Adaptation** | The PoC records drain progress in `drain_progress` and lets inbox rows expire through table TTL (default 24 h). It does not delete drained partitions. | TTL expiry still creates tombstones, unlike a partition-range delete. | Confirm whether production should delete drained partitions, rely on TTL, or both. See Open Design Question 9. |
| **R-12** | Self-managed Cassandra cluster settings (RF, gc_grace, TWCS). | **Intentional Astra DB Adaptation** | The PoC uses Astra DB Serverless. Replication, compaction, and `gc_grace_seconds` are Astra-managed. | Operators cannot configure NetworkTopologyStrategy / TWCS / gc_grace in application DDL. | Confirm Astra-managed defaults are acceptable for production. |
| **R-13** | Downstream: deliver events as a deduplicated, time-ordered stream. | **Partially Met** | `FileWriterEventSink` writes JSONL to local disk with atomic `.tmp` → `.jsonl` rename. Demonstrates the output contract for the PoC. | Placeholder only — no shared storage, no downstream consumer, incomplete replay/partial-output contract. | See Open Design Question 10. |
| **R-14** | Restart-safe buffer; reception and processing decoupled. | **Partially Met** | Kafka redelivery after an uncommitted offset is handled by idempotent upsert. Drainer progress is restored while `drain_progress` rows exist. `LATEST` bootstrap intentionally skips historical rows. | History skip on `LATEST`. Possible duplicate sink output if progress write fails after sink write. `drain_progress` TTL 7 days can lose the cursor (L-06). | Confirm bootstrap mode and replay expectations for production. |
| **R-15** | A single background worker drains the oldest sealed window. | **Met** as a PoC operating constraint | Exactly one drainer-enabled instance is supported. No distributed ownership or locking. | Multiple drainer replicas would duplicate sink writes. | Confirm single-drainer is acceptable until a lock exists (L-01). |
| **R-16** | Late messages are re-bucketed into the current open window, not dropped. | **Intentional design deviation** | The PoC keeps deterministic natural event-time buckets and does **not** rebucket. `allowed_lateness` only delays sealing; it does not change `window_bucket`. | Leaves post-drain late-event processing unresolved (R-04, L-07). | See Open Questions 2–5. Do not implement rebucketing before clarification. |

---

## Open Design Questions

These questions should be resolved before scoping a production implementation:

1. **What is the realistic maximum lateness of an event?**
   Suggested categories: less than 60 seconds; several minutes; hours; unknown.

2. **Must an event arriving after its original bucket was drained always be forwarded automatically?**

3. **Is out-of-order output for such an event acceptable?**
   (The specification already says out of global order is acceptable.)

4. **Is manual operator replay acceptable, or must recovery be automatic?**

5. **Is deterministic natural-bucket assignment preferred over the original rebucketing proposal?**

6. **Is wall-clock sealing acceptable, or is a stream-derived event-time watermark mandatory?**

7. **Is the 1,000–2,000 msg/s peak:** sustained; a short burst; per topic; per partition; or total across the service?

8. **What is the required duration of peak load?**

9. **What is the required retention period for inbox rows?**
   (Default TTL is 24 hours.)

10. **What is the actual downstream sink contract?** — output naming; durability; atomic publication; duplicate handling; replay handling; partial output handling.

---

## Current PoC Position

- Do not implement late-event rebucketing before the design questions above are resolved.
- Do not claim throughput support before validation.
- Do not claim full specification compliance.

Related: [`known-limitations.md`](known-limitations.md) · [`architecture-engineering.md`](architecture-engineering.md)
