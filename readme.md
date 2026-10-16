# Real-Time Distributed Order Processing Platform

A production-grade, event-driven distributed order processing and delivery coordination platform built with Node.js, TypeScript, Apache Kafka, PostgreSQL, Redis, and Server-Sent Events (SSE).

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Core Engineering Highlights](#core-engineering-highlights)
- [Component Overview](#component-overview)
- [Distributed Patterns & Reliability](#distributed-patterns--reliability)
  - [1. Transactional Outbox Pattern](#1-transactional-outbox-pattern)
  - [2. Exactly-Once / At-Least-Once Processing & Idempotency](#2-exactly-once--at-least-once-processing--idempotency)
  - [3. Dead Letter Queue (DLQ) & Fault Tolerance](#3-dead-letter-queue-dlq--fault-tolerance)
  - [4. Distributed Locking & Leader Election](#4-distributed-locking--leader-election)
  - [5. Real-Time Event Fanout & SSE Backpressure](#5-real-time-event-fanout--sse-backpressure)
- [API Reference](#api-reference)
- [Observability & Metrics](#observability--metrics)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Configuration](#environment-configuration)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Database Schema & Migrations](#database-schema--migrations)
- [Verification & Load Testing](#verification--load-testing)
- [Project Structure](#project-structure)

---

## System Architecture

The platform decouples synchronous client requests from asynchronous order lifecycle operations, driver assignments, notification deliveries, and real-time frontend updates through an event-driven architecture.

```mermaid
flowchart TB
    subgraph Clients["Clients & Upstream"]
        Web[Web / Mobile Client]
        Ops[Operations Dashboard]
    end

    subgraph API["Order API Service (Fastify / TypeScript)"]
        Routes[HTTP Routes & Validation]
        OrderSvc[Order Service]
        SSEMgr[SSE Manager & Backpressure Engine]
        Relay[Outbox Polling Relay]
        Election[Leader Election Module]
    end

    subgraph Storage["Persistent & Coordination Storage"]
        PG[(PostgreSQL 16\nOrders, Outbox, Processed Events)]
        Redis[(Redis 7\nCache, Distributed Locks, Pub/Sub, Leases)]
    end

    subgraph MessageBus["Event Streaming Backbone"]
        Kafka{{Apache Kafka\norder-events, dlq-topic}}
    end

    subgraph Workers["Asynchronous Workers"]
        AssignWorker[Driver Assignment Worker]
        NotifyWorker[Notification Worker]
    end

    subgraph Telemetry["Observability Stack"]
        Prometheus[Prometheus Metrics Scraper]
        Grafana[Grafana Dashboards]
    end

    Web -->|POST /orders, PATCH /status| Routes
    Web <-->|GET /orders/:id/stream SSE| SSEMgr
    Ops -->|PromQL Queries| Grafana

    Routes --> OrderSvc
    OrderSvc -->|ACID Transaction: Order + Outbox Row| PG
    OrderSvc <-->|Cache-Aside & Mutex Locks| Redis

    Election <-->|Renewable Heartbeat Lease| Redis
    Relay -.->|When Leader| PG
    Relay -->|Publish Committed Events| Kafka

    Kafka -->|Consume Events - Partitioned by orderId| AssignWorker
    Kafka -->|Consume Events| NotifyWorker

    AssignWorker -->|Idempotent Processing & Driver Assign| PG
    AssignWorker -->|Poison Pills / Exhausted Retries| Kafka

    NotifyWorker -->|Publish Realtime Update| Redis
    Redis -.->|Pub/Sub Fanout Across API Instances| SSEMgr
    SSEMgr -->|Stream Status Events| Web

    API -.->|/metrics| Prometheus
    Workers -.->|/metrics| Prometheus
    Prometheus --> Grafana
```

---

## Core Engineering Highlights

- **Dual-Write Elimination**: Implements the **Transactional Outbox Pattern** within PostgreSQL transactions, guaranteeing that business entity updates and domain events are committed atomically without distributed 2PC overhead.
- **Strict Partition Ordering**: Kafka topic partitions are keyed by `orderId`, enforcing strict FIFO processing order for status transitions belonging to any individual order.
- **Consumer Deduplication**: Multi-tier idempotency backed by database constraints and a dedicated `processed_events` log ensures at-least-once message delivery produces exactly-once side effects.
- **Resilient Retry & Dead Letter Queue (DLQ)**: Non-blocking exponential backoff with full jitter and poison-pill detection routes unrecoverable messages to an audit-ready DLQ topic without stalling partition processing.
- **Distributed Synchronization**: Redis-backed atomic distributed mutexes (using Lua check-and-delete scripts) prevent race conditions on concurrent status mutations, while lease-based leader election guarantees single-active outbox relay execution across horizontal API replicas.
- **Backpressure-Managed SSE**: Full-duplex SSE streaming engine with socket-level kernel buffer tracking, configurable high-water marks, and drop policies preventing Node.js process Out-Of-Memory (OOM) failures under slow consumer scenarios.
- **End-to-End Observability**: Custom Prometheus metrics tracking HTTP request latency, outbox lag, Kafka consumer throughput, lock contention, SSE connection states, and buffer saturation with pre-provisioned Grafana dashboards.

---

## Component Overview

| Component | Technology | Responsibility |
|---|---|---|
| **API Gateway & Core Service** | Fastify, TypeScript | Validates incoming requests, computes pricing/tax, manages state machine transitions, and streams SSE updates. |
| **Transactional Outbox Relay** | Node.js, `pg`, KafkaJS | Polls pending outbox records using `FOR UPDATE SKIP LOCKED`, batches events to Kafka, and confirms publication. |
| **Order Assignment Worker** | Node.js, KafkaJS | Consumes `ORDER_CREATED` events, simulates driver discovery with exponential backoff, and updates orders to `ASSIGNED`. |
| **Notification Worker** | Node.js, KafkaJS, ioredis | Consumes order lifecycle events, sends dispatch notifications, and publishes bridge messages to Redis Pub/Sub for SSE fanout. |
| **Primary Relational Store** | PostgreSQL 16 | Stores orders, order items, audit history, outbox events, and idempotency ledger. |
| **In-Memory Coordination** | Redis 7 | Provides caching, sliding-window rate limiting, distributed locking, leader leases, and pub/sub messaging. |
| **Event Broker** | Apache Kafka (KRaft mode) | Durable, partitioned, replicated commit log for all asynchronous domain events. |
| **Monitoring & Dashboards** | Prometheus & Grafana | Real-time metrics collection, alerting hooks, and pre-configured operational dashboards. |

---

## Distributed Patterns & Reliability

### 1. Transactional Outbox Pattern

To eliminate the dual-write problem (where updating the database succeeds but publishing to the message broker fails or vice versa), all domain events are written to an `outbox_events` table inside the exact same database transaction that persists the order:

```sql
BEGIN;
  INSERT INTO orders (...) VALUES (...) RETURNING id;
  INSERT INTO order_items (...) VALUES (...);
  INSERT INTO outbox_events (aggregate_id, event_type, topic, key, payload, headers, status)
  VALUES (..., 'ORDER_CREATED', 'order-events', order_id, '{...}', '{...}', 'PENDING');
COMMIT;
```

#### Polling Relay with Atomic Status Claim
The outbox relay queries the database on a configurable interval. To prevent multiple workers from claiming the same event batch while avoiding prolonged database locks during network I/O to Kafka, an atomic status claim transition is executed:

```sql
UPDATE outbox_events
SET status = 'PROCESSING', locked_at = NOW()
WHERE id IN (
    SELECT id FROM outbox_events
    WHERE published_at IS NULL
      AND (status IS NULL OR status = 'PENDING' OR (status = 'PROCESSING' AND locked_at < NOW() - INTERVAL '30 seconds'))
    ORDER BY created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

Once Kafka successfully acknowledges receipt (`acks = -1`), the relay updates `published_at = NOW()` and `status = 'PUBLISHED'`.

---

### 2. Exactly-Once / At-Least-Once Processing & Idempotency

Kafka provides **at-least-once** delivery guarantees. The platform achieves effective **exactly-once processing semantics** at consumer boundaries using a multi-layered deduplication strategy:

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka Partition
    participant C as Order Consumer
    participant DB as PostgreSQL (processed_events)
    participant L as Business Logic

    K->>C: Deliver Message (eventId, orderId, payload)
    C->>DB: Check if eventId exists in processed_events
    alt Event already processed
        DB-->>C: Record Found (Duplicate)
        C->>K: Commit Offset (Skip side-effects)
    else Event not yet processed
        DB-->>C: Record Not Found
        C->>L: Execute Business Logic (e.g. Driver Assignment)
        C->>DB: Atomically persist state change + INSERT INTO processed_events
        C->>K: Commit Offset
    end
```

Database-level unique constraints on `processed_events.event_id` and `orders.idempotency_key` guarantee that concurrent duplicates or replayed messages are safely rejected without corrupting application state.

---

### 3. Dead Letter Queue (DLQ) & Fault Tolerance

Transient failures (e.g., downstream network timeout) are differentiated from poison pills (corrupt payload, unrecoverable domain violation):

1. **Transient Errors**: Retried with exponential backoff and randomized jitter:
   $$t_{\text{wait}} = \min(t_{\text{max}}, t_{\text{initial}} \times 2^{\text{attempt}}) \pm \text{jitter}$$
2. **Poison Pills / Max Retries Exceeded**: When a message fails after $N$ attempts (default: 3), it is routed to the Dead Letter Queue topic (`order-events-dlq`) with full diagnostic headers (`x-orig-topic`, `x-exception-message`, `x-retry-count`, `x-failed-at`).
3. **Partition Continuity**: The consumer commits the offset of the failed message after DLQ routing, preventing an unprocessable message from blocking partition progress for subsequent orders.

---

### 4. Distributed Locking & Leader Election

#### Safe Mutex Lock Acquisition and Release
To prevent race conditions during concurrent status transitions across multiple API instances, a distributed lock is acquired over `lock:order:<orderId>`:

- **Acquire**: `SET lock:<resource> <unique_token> NX PX <ttl_ms>`
- **Atomic Release via Lua**:
  ```lua
  if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
  else
      return 0
  end
  ```
  Verifying the unique token prevents a slow process whose lock expired from mistakenly releasing a lock acquired by another process.

#### Renewable Lease Leader Election
Only one API replica should run the outbox relay background polling loop to minimize redundant database load. The platform uses Redis renewable leases:

- **Acquire Leadership**: `SET leader:outbox-relay <instanceId> NX PX 10000`
- **Heartbeat Renewal**: Evaluated periodically via Lua to extend the lease TTL only if the current instance remains the owner.
- **Failover**: If the leader crashes, the lease expires after 10 seconds, allowing an active follower to assume leadership seamlessly.

---

### 5. Real-Time Event Fanout & SSE Backpressure

```mermaid
flowchart LR
    subgraph Instances["Distributed API Layer"]
        API1["API Instance 1\n(Client Order A)"]
        API2["API Instance 2\n(Client Order B)"]
    end

    subgraph RedisPubSub["Redis Pub/Sub Channel"]
        ChA["op:sse:order:A"]
        ChB["op:sse:order:B"]
    end

    Worker[Notification Consumer] -->|PUBLISH op:sse:order:A| ChA
    Worker -->|PUBLISH op:sse:order:B| ChB

    ChA -->|Subscribe| API1
    ChB -->|Subscribe| API2

    API1 -->|Stream SSE Event| ClientA[Client A Browser]
    API2 -->|Stream SSE Event| ClientB[Client B Browser]
```

#### Kernel-Level Backpressure Management
Server-Sent Events run over persistent HTTP connections. A slow client on high latency or throttled network can cause Node.js userspace buffers to expand without bound when events are dispatched quickly.

The custom `BackpressureTracker`:
1. Inspects the boolean return value of `response.write(chunk)`.
2. Tracks `bufferedBytes` and hooks into the Node.js `'drain'` stream event.
3. Enforces configurable thresholds:
   - **High Water Mark (64 KB)**: Emits operational metrics and diagnostic warnings.
   - **Critical Mark (256 KB)**: Triggers the configured `BackpressurePolicy` (`DROP_NEWEST`, `DROP_OLDEST`, or `DISCONNECT`).

---

## API Reference

### Base URL: `http://localhost:3000/api/v1`

### 1. Create Order
Creates a new order and atomically enqueues an `ORDER_CREATED` outbox event. Supports idempotency via header.

- **Endpoint**: `POST /orders`
- **Headers**:
  - `Content-Type: application/json`
  - `Idempotency-Key: <unique-uuid>` *(optional, recommended)*
- **Request Body**:
  ```json
  {
    "customerId": "cust_9876",
    "customerName": "Alice Johnson",
    "customerEmail": "alice@example.com",
    "customerPhone": "+15551234567",
    "restaurantId": "rest_102",
    "restaurantName": "Artisan Pizza Co.",
    "deliveryAddress": "456 Market St, Suite 200, San Francisco, CA",
    "items": [
      { "itemName": "Margherita Pizza", "quantity": 2, "unitPrice": 18.50 },
      { "itemName": "San Pellegrino", "quantity": 2, "unitPrice": 3.50 }
    ]
  }
  ```
- **Response**: `201 Created`
  ```json
  {
    "success": true,
    "data": {
      "id": "e3052bb2-f8b8-4c28-98e3-a4e370f11181",
      "customerId": "cust_9876",
      "status": "PENDING",
      "totalAmount": 44.00,
      "deliveryFee": 0.00,
      "taxAmount": 3.52,
      "grandTotal": 47.52,
      "createdAt": "2026-10-17T01:30:00.000Z"
    }
  }
  ```

---

### 2. Get Order Details
Retrieves current order state. Utilizes Redis cache-aside caching (60s TTL).

- **Endpoint**: `GET /orders/:id`
- **Response**: `200 OK`
  ```json
  {
    "success": true,
    "data": {
      "id": "e3052bb2-f8b8-4c28-98e3-a4e370f11181",
      "status": "CONFIRMED",
      "customerName": "Alice Johnson",
      "restaurantName": "Artisan Pizza Co.",
      "grandTotal": 47.52,
      "items": [...]
    }
  }
  ```

---

### 3. Update Order Status
Transitions the order status according to the valid state machine:
`PENDING` $\rightarrow$ `CONFIRMED` $\rightarrow$ `PREPARING` $\rightarrow$ `OUT_FOR_DELIVERY` $\rightarrow$ `DELIVERED` (or `CANCELLED`).

- **Endpoint**: `PATCH /orders/:id/status`
- **Request Body**:
  ```json
  {
    "status": "CONFIRMED",
    "changedBy": "restaurant_portal",
    "reason": "Kitchen confirmed order ticket"
  }
  ```
- **Response**: `200 OK`

---

### 4. Real-Time Order Stream (SSE)
Establishes a persistent Server-Sent Events stream for real-time order lifecycle events.

- **Endpoint**: `GET /orders/:id/stream`
- **Headers**:
  - `Accept: text/event-stream`
- **Protocol Flow**:
  ```text
  HTTP/1.1 200 OK
  Content-Type: text/event-stream
  Cache-Control: no-cache, no-transform
  Connection: keep-alive
  Access-Control-Allow-Origin: *

  event: connected
  data: {"connectionId":"21a719bf-e3b9-4b68-80f0-80eaae5f7bf4","orderId":"e3052bb2...","message":"SSE stream established"}
  retry: 3000

  event: initial_state
  data: {"orderId":"e3052bb2...","status":"PENDING","grandTotal":47.52}

  event: status_update
  data: {"orderId":"e3052bb2...","previousStatus":"PENDING","newStatus":"CONFIRMED"}

  : heartbeat 2026-10-17T01:30:15.000Z
  ```

---

### 5. Health Probes
- `GET /health`: Liveness probe verifying process responsiveness.
- `GET /health/ready`: Readiness probe validating PostgreSQL, Redis, Kafka producer, and consumer connection status.

---

## Observability & Metrics

Prometheus scrapes metrics from the `/metrics` endpoint on port `3000`.

### Key Production Metrics

| Metric Name | Type | Description |
|---|---|---|
| `http_requests_total` | Counter | Total incoming HTTP requests partitioned by method, route, and status code. |
| `http_request_duration_seconds` | Histogram | Request latency distribution across all API endpoints. |
| `outbox_events_relayed_total` | Counter | Outbox events successfully published to Kafka vs. failures. |
| `outbox_relay_poll_duration_seconds` | Histogram | Execution time of outbox batch polling queries. |
| `outbox_events_pending` | Gauge | Number of unprocessed outbox records waiting to be published. |
| `kafka_consumer_messages_processed_total` | Counter | Consumer message throughput by topic, group, and status (`success`, `duplicate`, `error`, `dlq`). |
| `kafka_consumer_processing_duration_seconds` | Histogram | End-to-end message processing time inside worker handlers. |
| `kafka_consumer_dlq_messages_total` | Counter | Total poison pill messages routed to Dead Letter Queue. |
| `redis_operations_total` | Counter | Redis command counts by operation type (`lock_acquire`, `cache_hit`, `cache_miss`). |
| `sse_connections_active` | Gauge | Currently open active Server-Sent Event client connections. |
| `sse_events_sent_total` | Counter | Real-time events pushed to connected SSE streams. |
| `sse_backpressure_events_total` | Counter | Backpressure mitigation events triggered (`drop`, `disconnect`). |

### Grafana Dashboards
Pre-provisioned dashboards are available in `monitoring/grafana/dashboards/order-platform.json`, presenting real-time panels for:
- API Request Rates, P95/P99 Latency & Error Budgets
- Outbox Relay Lag & Kafka Publishing Throughput
- Consumer Processing Latencies & DLQ Incident Tracking
- Redis Cache Hit Ratios & Distributed Lock Contention
- SSE Active Streams, Buffer Saturation & Backpressure Actions

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)
- [Node.js](https://nodejs.org/) v20+ *(for local development without containers)*
- `curl`, `jq`, and `bash` *(for load testing scripts)*

---

### Environment Configuration

Copy the template environment file:

```bash
cp .env.example .env
```

Default configuration (`.env`):
```env
PORT=3000
NODE_ENV=development
LOG_LEVEL=info

# PostgreSQL
DATABASE_URL=postgres://postgres:postgres@postgres:5432/order_platform
PG_POOL_MIN=2
PG_POOL_MAX=20

# Apache Kafka
KAFKA_BROKERS=kafka:9092
KAFKA_CLIENT_ID=order-platform
KAFKA_TOPIC_ORDER_EVENTS=order-events
KAFKA_TOPIC_ORDER_EVENTS_DLQ=order-events-dlq
KAFKA_CONSUMER_GROUP_ORDERS=order-processing-group
KAFKA_CONSUMER_GROUP_NOTIFICATIONS=notification-group

# Redis
REDIS_URL=redis://redis:6379
REDIS_KEY_PREFIX=op:

# Engine Knobs
OUTBOX_POLL_INTERVAL_MS=500
OUTBOX_BATCH_SIZE=100
LEADER_ELECTION_HEARTBEAT_MS=3000
LEADER_ELECTION_LEASE_TTL_MS=10000
SSE_HEARTBEAT_INTERVAL_MS=15000
SSE_MAX_CONNECTIONS=1000
```

---

### Running with Docker Compose

Spin up the entire distributed topology (PostgreSQL, Kafka, Redis, Order API, Assignment Consumer, Notification Consumer, Prometheus, and Grafana) with a single command:

```bash
docker compose up --build -d
```

Verify service status:
```bash
docker compose ps
```

Access exposed endpoints:
- **Order API**: `http://localhost:3000`
- **Prometheus UI**: `http://localhost:9090`
- **Grafana Dashboard**: `http://localhost:3001` *(Default credentials: `admin` / `admin`)*

---

### Database Schema & Migrations

The database initialization script runs automatically on first boot via `scripts/init-db.sql`. It configures:
- Tables: `orders`, `order_items`, `order_status_history`, `outbox_events`, `processed_events`.
- Performance indices on foreign keys, `idempotency_key`, `outbox_events(status, published_at, created_at)`, and `processed_events(event_id)`.

To re-run migrations manually:
```bash
docker compose exec -T postgres psql -U postgres -d order_platform < scripts/init-db.sql
```

---

## Verification & Load Testing

### 1. Basic Health Check
```bash
curl -s http://localhost:3000/health/ready | jq .
```

### 2. End-to-End Order Creation & Real-Time Stream

Terminal 1 — Listen to the real-time SSE stream:
```bash
# Place an order and capture ID
ORDER_ID=$(curl -s -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust_123",
    "customerName": "John Doe",
    "customerEmail": "john@example.com",
    "restaurantId": "rest_1",
    "restaurantName": "Burger Bar",
    "deliveryAddress": "123 Main St",
    "items": [{"itemName": "Cheeseburger", "quantity": 1, "unitPrice": 12.00}]
  }' | jq -r '.data.id')

echo "Streaming updates for Order: $ORDER_ID"
curl -N http://localhost:3000/api/v1/orders/$ORDER_ID/stream
```

Terminal 2 — Transition order status:
```bash
curl -X PATCH http://localhost:3000/api/v1/orders/$ORDER_ID/status \
  -H "Content-Type: application/json" \
  -d '{"status": "CONFIRMED", "changedBy": "kitchen"}'
```
*Observe the instant `status_update` push in Terminal 1.*

---

### 3. Automated Concurrent Load Test

Execute the bundled high-concurrency load testing script:

```bash
chmod +x scripts/load-test.sh
./scripts/load-test.sh
```

Parameters can be adjusted via environment variables:
```bash
TOTAL_ORDERS=200 CONCURRENCY=20 ./scripts/load-test.sh
```

The script verifies:
- End-to-end order creation rate and latency
- Outbox relay throughput and drain rates
- Asynchronous worker driver assignments
- Kafka consumer idempotency under duplicate bursts

---

## Project Structure

```text
distributed-order-processing/
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── readme.md
├── scripts/
│   ├── init-db.sql              # PostgreSQL DDL schema & indexes
│   └── load-test.sh             # Concurrency and throughput load harness
├── monitoring/
│   ├── prometheus.yml           # Prometheus scrape configuration
│   └── grafana/
│       ├── provisioning/        # Datasource & dashboard auto-providers
│       └── dashboards/
│           └── order-platform.json
└── src/
    ├── index.ts                 # Application entry point & service bootstrap
    ├── server.ts                # Fastify HTTP server instantiation
    ├── config/                  # Validated runtime configuration
    ├── shared/                  # Common error types, logging, health probes
    ├── metrics/                 # Prometheus collectors & HTTP metrics middleware
    ├── middleware/              # Sliding-window Redis rate limiter
    ├── modules/
    │   ├── orders/              # Order routes, domain service, and repository
    │   └── notifications/       # Notification delivery service
    ├── outbox/
    │   ├── outbox.repository.ts # Transactional outbox persistence
    │   └── outbox-relay.ts      # Polling relay publisher
    ├── kafka/
    │   ├── client.ts            # KafkaJS client factory & topic provisioning
    │   ├── producer.ts          # Event producer with partition keying
    │   ├── consumer.ts          # Resilient consumer runner with retries
    │   ├── retry.ts             # Exponential backoff with full jitter
    │   ├── idempotency.ts       # Database-backed event deduplication
    │   ├── dlq-producer.ts      # Dead Letter Queue publisher
    │   └── events.ts            # Typed domain event schemas
    ├── consumers/               # Background Kafka consumer worker processes
    ├── redis/
    │   ├── client.ts            # Redis connection management
    │   ├── cache.ts             # Cache-aside helper
    │   ├── distributed-lock.ts  # Lua-backed distributed mutex
    │   └── leader-election.ts   # Lease-based leader election
    └── sse/
        ├── sse-connection.ts    # Individual client SSE stream wrapper
        ├── sse-manager.ts       # Connection registry & broadcast engine
        ├── sse-backpressure.ts  # Stream buffer tracker & drop policy
        ├── redis-subscriber.ts  # Redis Pub/Sub multi-instance fanout bridge
        ├── sse-routes.ts        # Fastify hijacked SSE streaming route
        └── types.ts             # Protocol & event type declarations
```

---

## License

This project is licensed under the MIT License.
