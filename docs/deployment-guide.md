# Deployment Guide

Covers deploying `astra-event-inbox` beyond the local development environment.

> **PoC scope:** Kubernetes manifests and CI/CD pipelines are not yet included. This guide documents a target model and manual steps.

---

## Deployment topologies

| Topology | Status | Notes |
|---|---|---|
| Local Docker Compose | ✅ Ready | Development and demo use |
| Single Docker container (any host) | ✅ Ready | Manual setup required |
| Kubernetes (Helm / Kustomize) | ❌ Not yet implemented | See [future-enhancements.md](future-enhancements.md) |
| Cloud-managed Kafka (MSK, Confluent Cloud) | ✅ Config-only | Change `KAFKA_BOOTSTRAP_SERVERS` and add TLS/SASL config |

---

## 1. Prerequisites

Before deploying to any environment:

1. **Astra DB keyspace exists** — create via Astra portal or CLI:
   ```bash
   astra db create-keyspace <db-name> -k tds_inbox
   ```

2. **Schema applied** — run once per keyspace:
   ```bash
   cqlsh -u <client_id> -p <client_secret> \
     --secure-connect-bundle /path/to/bundle.zip \
     -k tds_inbox \
     -f src/main/resources/schema.cql
   ```
   `schema-phase2.cql` is applied automatically by `SchemaInitializer` at application startup.

3. **Kafka topics exist** — required topics:
   - `cloud-events` (6 partitions recommended; match `KAFKA_CONSUMER_CONCURRENCY`)
   - `cloud-events.DLT` (1 partition)

---

## 2. Build the Docker image

Build locally:

```bash
docker build -t astra-event-inbox:latest .
```

Tag for a registry:

```bash
docker tag astra-event-inbox:latest <registry>/astra-event-inbox:latest
```

Push to registry:

```bash
docker push <registry>/astra-event-inbox:latest
```

The image uses a **multi-stage build**:
- Stage 1: `maven:3.9-eclipse-temurin-21` — builds the fat JAR
- Stage 2: `eclipse-temurin:21-jre-jammy` — minimal runtime, non-root user `inbox`

---

## 3. Required environment variables

The following variables **must** be set at runtime. There are no defaults.

| Variable | Description |
|---|---|
| `ASTRA_SECURE_BUNDLE_PATH` | Path to the Secure Connect Bundle ZIP **inside the container** |
| `ASTRA_CLIENT_ID` | Astra service account client ID |
| `ASTRA_CLIENT_SECRET` | Astra service account client secret |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers (e.g. `kafka:9092` or MSK endpoint) |

### Mounting the Secure Connect Bundle

The SCB must be available inside the container. Two approaches:

**Option A — Volume mount (recommended for VMs and non-Kubernetes):**
```bash
docker run \
  -v /secure/path/secure-connect.zip:/app/certs/bundle.zip:ro \
  -e ASTRA_SECURE_BUNDLE_PATH=/app/certs/bundle.zip \
  ...
```

**Option B — Baked into image (for controlled environments only):**
```dockerfile
COPY secure-connect.zip /app/certs/bundle.zip
```
> ⚠️ Only bake credentials into images in environments where the registry is private and access-controlled. Never push credential-containing images to public registries.

---

## 4. Run as a standalone Docker container

```bash
docker run -d \
  --name astra-event-inbox \
  --restart unless-stopped \
  -p 8080:8080 \
  -v /path/to/secure-connect.zip:/app/certs/bundle.zip:ro \
  -e ASTRA_SECURE_BUNDLE_PATH=/app/certs/bundle.zip \
  -e ASTRA_CLIENT_ID=<client-id> \
  -e ASTRA_CLIENT_SECRET=<client-secret> \
  -e ASTRA_KEYSPACE=tds_inbox \
  -e KAFKA_BOOTSTRAP_SERVERS=<kafka-host>:9092 \
  -e KAFKA_TOPIC=cloud-events \
  -e KAFKA_DLT_TOPIC=cloud-events.DLT \
  -e KAFKA_CONSUMER_CONCURRENCY=6 \
  -e INBOX_WINDOW_SIZE_SECONDS=5 \
  astra-event-inbox:latest
```

Verify:
```bash
curl http://<host>:8080/actuator/health
```

---

## 5. Run with Docker Compose (full stack)

The provided `docker-compose.yml` starts Kafka (KRaft) + topic init + the application in one command:

Copy and fill in credentials:

```bash
cp .env.example .env
```

Start:

```bash
docker compose up -d
```

View logs:

```bash
docker compose logs -f inbox
```

Stop:

```bash
docker compose down
```

For environments where Kafka is managed externally, use the application service only and point `KAFKA_BOOTSTRAP_SERVERS` at the existing cluster.

---

## 6. Target Kubernetes deployment (future)

Until Helm/Kustomize manifests exist, the following blueprint describes the intended Kubernetes resource model.

### Resource sketch

```yaml
# Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: astra-event-inbox
spec:
  replicas: 1   # Only 1 drainer replica until distributed locking is implemented
  template:
    spec:
      containers:
        - name: inbox
          image: <registry>/astra-event-inbox:latest
          ports:
            - containerPort: 8080
          envFrom:
            - secretRef:
                name: astra-event-inbox-secrets
          env:
            - name: ASTRA_SECURE_BUNDLE_PATH
              value: /app/certs/bundle.zip
          volumeMounts:
            - name: astra-bundle
              mountPath: /app/certs
              readOnly: true
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
          # Current PoC: /readiness is Spring readinessState only (not Astra/Kafka).
          # To wait for dependencies, probe /actuator/health or configure a health group.
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
      volumes:
        - name: astra-bundle
          secret:
            secretName: astra-secure-connect-bundle
```

```yaml
# Secret (Astra credentials)
apiVersion: v1
kind: Secret
metadata:
  name: astra-event-inbox-secrets
type: Opaque
stringData:
  ASTRA_CLIENT_ID: "<client-id>"
  ASTRA_CLIENT_SECRET: "<client-secret>"
  ASTRA_KEYSPACE: tds_inbox
  KAFKA_BOOTSTRAP_SERVERS: "<kafka-host>:9092"
```

```yaml
# Secret (SCB file as binary)
apiVersion: v1
kind: Secret
metadata:
  name: astra-secure-connect-bundle
type: Opaque
data:
  bundle.zip: <base64-encoded-zip>
```

Create the bundle secret:
```bash
kubectl create secret generic astra-secure-connect-bundle \
  --from-file=bundle.zip=/path/to/secure-connect.zip
```

### Important scaling constraint

> **Run only ONE replica when `INBOX_DRAINER_ENABLED=true`.** Distributed locking for the drainer is not yet implemented. Multiple drainer replicas will produce duplicate drain processing. Set `replicas: 1` until the distributed lock enhancement is implemented (see [future-enhancements.md](future-enhancements.md)).

The **ingestion path** (writer-only mode, `INBOX_DRAINER_ENABLED=false`) is safe to scale horizontally because each consumer thread handles its own Kafka partition independently, and Astra writes are idempotent.

---

## 7. Configuration for managed Kafka (MSK / Confluent Cloud)

For Kafka services that require TLS and SASL authentication, add the following environment variables:

SASL/SCRAM (Confluent Cloud style):

```bash
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=PLAIN
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";
```

MSK with IAM auth:

```bash
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=AWS_MSK_IAM
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=software.amazon.msk.auth.iam.IAMLoginModule required;
SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

---

## 8. Health and readiness checks

Spring Boot Actuator exposes:

| Endpoint | Use |
|---|---|
| `GET /actuator/health/liveness` | Kubernetes `livenessProbe` — Spring `livenessState` only |
| `GET /actuator/health/readiness` | Spring `readinessState` only — application has started. Does **not** include Astra or Kafka unless a health group is configured to include those indicators |
| `GET /actuator/health` | Aggregate health including Astra and Kafka indicators. Use this (or an explicit group) if the orchestrator must wait for dependencies |
| `GET /actuator/metrics` | Micrometer metric metadata and measurements as **JSON**. Not a Prometheus scrape target |
| `GET /actuator/prometheus` | Available only after adding and configuring `micrometer-registry-prometheus` |

The current PoC does **not** put Astra or Kafka into the readiness group. `/actuator/health/readiness` therefore does not return DOWN solely because Astra or Kafka is unhealthy. Those indicators roll up into `/actuator/health`.

Compose healthcheck uses `/actuator/health`.

---

## 9. Monitoring setup

### Prometheus (not yet wired)

Add the Prometheus registry to `pom.xml`:

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Add to `application.yml`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

Scrape endpoint: `GET /actuator/prometheus`

### Key metrics to alert on

| Metric | Alert condition | Meaning |
|---|---|---|
| `inbox.events.astra_error` | Rate > 0 for > 60 s | Persistent Astra write failures |
| `inbox.events.parse_error` | Rate > 0 | Malformed events arriving; investigate upstream producer |
| `inbox.events.late_arrival` | Rate > 0 | Events arriving after drainer has moved past their window |
| Kafka consumer lag | Lag > `INBOX_KAFKA_HEALTH_MAX_LAG` (10,000) | Consumer falling behind; scale or investigate |

---

## 10. Operational runbook

### Restart the service

Docker Compose:

```bash
docker compose restart inbox
```

Standalone Docker:

```bash
docker restart astra-event-inbox
```

Kubernetes:

```bash
kubectl rollout restart deployment/astra-event-inbox
```

On restart:
- Kafka consumer resumes from the last committed offset. At most `KAFKA_MAX_POLL_RECORDS × p99_astra_latency` messages are redelivered. All upserts are idempotent.
- Drainer restores its cursor from `drain_progress`. No buckets are re-drained unless the cursor row has expired (7-day TTL).

### Force a historical replay

To reprocess events from a specific time:

1. Stop the service.
2. Set `INBOX_DRAINER_BOOTSTRAP_MODE=CONFIGURED` and `INBOX_DRAINER_START_BUCKET=<epoch-second-bucket>`.
3. Manually delete the `drain_progress` rows for the drain ID (or change `INBOX_DRAINER_DRAIN_ID` to a new value).
4. Start the service.

### Inspect dead-letter messages

```bash
docker exec inbox-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cloud-events.DLT \
  --from-beginning \
  --property print.headers=true
```

Each DLT message retains the original headers and value for debugging.

### Check partition lag

```bash
docker exec inbox-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group astra-event-inbox
```

Or via the health endpoint:

```bash
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
```
