// ============================================================
// Metrics Registry — Prometheus Metric Definitions
// ============================================================

import client from 'prom-client';

export const register = client.register;

export function collectDefaultMetrics(): void {
    client.collectDefaultMetrics({
        register,
        prefix: '',
    });
}

// ─────────────────────────────────────────────────
// HTTP Metrics (RED Method)
// ─────────────────────────────────────────────────

export const httpRequestsTotal = new client.Counter({
    name: 'http_requests_total',
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'route', 'status_code'] as const,
});

export const httpRequestDurationSeconds = new client.Histogram({
    name: 'http_request_duration_seconds',
    help: 'HTTP request latency in seconds',
    labelNames: ['method', 'route', 'status_code'] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
});

export const httpRequestsInFlight = new client.Gauge({
    name: 'http_requests_in_flight',
    help: 'Number of HTTP requests currently being processed',
});

// ─────────────────────────────────────────────────
// Kafka Consumer Metrics
// ─────────────────────────────────────────────────

export const kafkaConsumerMessagesTotal = new client.Counter({
    name: 'kafka_consumer_messages_total',
    help: 'Total Kafka messages processed by consumer',
    labelNames: ['topic', 'status'] as const,
});

export const kafkaConsumerProcessingDurationSeconds = new client.Histogram({
    name: 'kafka_consumer_processing_duration_seconds',
    help: 'Time to process a single Kafka message in seconds',
    labelNames: ['topic'] as const,
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
});

// ─────────────────────────────────────────────────
// Outbox Relay Metrics
// ─────────────────────────────────────────────────

export const outboxEventsRelayedTotal = new client.Counter({
    name: 'outbox_events_relayed_total',
    help: 'Total outbox events relayed to Kafka',
    labelNames: ['status'] as const,
});

export const outboxRelayPollDurationSeconds = new client.Histogram({
    name: 'outbox_relay_poll_duration_seconds',
    help: 'Duration of a single outbox relay poll cycle in seconds',
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
});

export const outboxEventsPending = new client.Gauge({
    name: 'outbox_events_pending',
    help: 'Number of unpublished events in the outbox table',
});

// ─────────────────────────────────────────────────
// Redis Metrics
// ─────────────────────────────────────────────────

export const redisOperationsTotal = new client.Counter({
    name: 'redis_operations_total',
    help: 'Total Redis operations by type',
    labelNames: ['operation'] as const,
});

export const redisOperationDurationSeconds = new client.Histogram({
    name: 'redis_operation_duration_seconds',
    help: 'Redis operation latency in seconds',
    labelNames: ['operation'] as const,
    buckets: [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5],
});

// ─────────────────────────────────────────────────
// SSE Metrics
// ─────────────────────────────────────────────────

export const sseConnectionsActive = new client.Gauge({
    name: 'sse_connections_active',
    help: 'Current number of active SSE connections',
});

export const sseEventsSentTotal = new client.Counter({
    name: 'sse_events_sent_total',
    help: 'Total SSE events sent to clients',
    labelNames: ['event_type'] as const,
});

export const sseBackpressureEventsTotal = new client.Counter({
    name: 'sse_backpressure_events_total',
    help: 'Total SSE backpressure events',
    labelNames: ['action'] as const,
});
