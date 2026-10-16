// ============================================================
// Metrics Module — Barrel Export
// ============================================================

export { register, collectDefaultMetrics } from './metrics.js';
export { registerMetricsPlugin } from './http-metrics.js';

// Re-export all metric instances for instrumentation points
export {
    // HTTP
    httpRequestsTotal,
    httpRequestDurationSeconds,
    httpRequestsInFlight,

    // Kafka Consumer
    kafkaConsumerMessagesTotal,
    kafkaConsumerProcessingDurationSeconds,

    // Outbox Relay
    outboxEventsRelayedTotal,
    outboxRelayPollDurationSeconds,
    outboxEventsPending,

    // Redis
    redisOperationsTotal,
    redisOperationDurationSeconds,

    // SSE
    sseConnectionsActive,
    sseEventsSentTotal,
    sseBackpressureEventsTotal
} from './metrics.js';