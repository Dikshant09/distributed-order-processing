// ============================================================
// Configuration — Typed, Validated, Fail-Fast
// ============================================================

import { z } from 'zod';
import dotenv from 'dotenv';

// Load .env file in development (no-op if file doesn't exist)
dotenv.config();

const configSchema = z.object({
    // Server
    nodeEnv: z.enum(['development', 'production', 'test']).default('development'),
    port: z.coerce.number().int().positive().default(3000),
    host: z.string().default('0.0.0.0'),

    // Database
    databaseUrl: z.url({ error: "DATABASE_URL must be a valid URL" }),

    // Connection Pool
    dbPoolMin: z.coerce.number().int().min(0).default(2),
    dbPoolMax: z.coerce.number().int().min(1).default(10),
    dbPoolIdleTimeoutMs: z.coerce.number().int().positive().default(30000),
    dbConnectionTimeoutMs: z.coerce.number().int().positive().default(5000),

    // Logging
    logLevel: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

    // Notification simulation
    notificationDelayMs: z.coerce.number().int().min(0).default(500),
    notificationFailureRate: z.coerce.number().min(0).max(1).default(0),

    // Kafka
    kafkaBrokers: z.string().default(''), // comma-separated, e.g. "kafka:9092"
    kafkaClientId: z.string().default('order-platform'),
    kafkaConsumerGroupId: z.string().default('notification-service'),
    kafkaTopicOrderEvents: z.string().default('order-events'),

    // Kafka Dead Letter Queue
    kafkaTopicDlq: z.string().default('order-events-dlq'),

    // Consumer retry
    consumerMaxRetries: z.coerce.number().int().min(0).default(3),
    consumerRetryBaseDelayMs: z.coerce.number().int().min(100).default(1000),

    // Outbox relay
    outboxPollIntervalMs: z.coerce.number().int().min(100).default(1000),
    outboxBatchSize: z.coerce.number().int().min(1).default(50),

    // Redis
    redisUrl: z.string().default(''),
    redisKeyPrefix: z.string().default('op:'),

    // Rate Limiting
    rateLimitWindowMs: z.coerce.number().int().min(1000).default(60000),
    rateLimitMaxRequests: z.coerce.number().int().min(1).default(30),

    // Cache
    cacheOrderTtlSeconds: z.coerce.number().int().min(1).default(60),

    // Leader Election
    leaderElectionLeaseTtlMs: z.coerce.number().int().min(1000).default(15000),
    leaderElectionHeartbeatMs: z.coerce.number().int().min(500).default(5000),

    // SSE / Realtime
    sseHeartbeatIntervalMs: z.coerce.number().int().min(1000).default(15000),
    sseMaxConnectionsPerOrder: z.coerce.number().int().min(1).default(10),
    sseMaxTotalConnections: z.coerce.number().int().min(1).default(1000),
    sseBackpressureHighWaterMark: z.coerce.number().int().min(1024).default(65536),
    sseBackpressureCriticalMark: z.coerce.number().int().min(4096).default(262144),
    sseClientRetryMs: z.coerce.number().int().min(500).default(3000),

    // Metrics / Observability
    metricsEnabled: z.coerce.boolean().default(true),
    metricsPath: z.string().default('/metrics'),
    consumerMetricsPort: z.coerce.number().int().min(1000).default(9464),

    // Graceful Shutdown
    shutdownTimeoutMs: z.coerce.number().int().positive().default(10000),
});

export type Config = z.infer<typeof configSchema>;

function loadConfig(): Config {
    const result = configSchema.safeParse({
        nodeEnv: process.env['NODE_ENV'],
        port: process.env['PORT'],
        host: process.env['HOST'],
        databaseUrl: process.env['DATABASE_URL'],
        dbPoolMin: process.env['DB_POOL_MIN'],
        dbPoolMax: process.env['DB_POOL_MAX'],
        dbPoolIdleTimeoutMs: process.env['DB_POOL_IDLE_TIMEOUT_MS'],
        dbConnectionTimeoutMs: process.env['DB_CONNECTION_TIMEOUT_MS'],
        logLevel: process.env['LOG_LEVEL'],
        notificationDelayMs: process.env['NOTIFICATION_DELAY_MS'],
        notificationFailureRate: process.env['NOTIFICATION_FAILURE_RATE'],
        kafkaBrokers: process.env['KAFKA_BROKERS'],
        kafkaClientId: process.env['KAFKA_CLIENT_ID'],
        kafkaConsumerGroupId: process.env['KAFKA_CONSUMER_GROUP_ID'],
        kafkaTopicOrderEvents: process.env['KAFKA_TOPIC_ORDER_EVENTS'],
        kafkaTopicDlq: process.env['KAFKA_TOPIC_DLQ'],
        consumerMaxRetries: process.env['CONSUMER_MAX_RETRIES'],
        consumerRetryBaseDelayMs: process.env['CONSUMER_RETRY_BASE_DELAY_MS'],
        outboxPollIntervalMs: process.env['OUTBOX_POLL_INTERVAL_MS'],
        outboxBatchSize: process.env['OUTBOX_BATCH_SIZE'],
        redisUrl: process.env['REDIS_URL'],
        redisKeyPrefix: process.env['REDIS_KEY_PREFIX'],
        rateLimitWindowMs: process.env['RATE_LIMIT_WINDOW_MS'],
        rateLimitMaxRequests: process.env['RATE_LIMIT_MAX_REQUESTS'],
        cacheOrderTtlSeconds: process.env['CACHE_ORDER_TTL_SECONDS'],
        leaderElectionLeaseTtlMs: process.env['LEADER_ELECTION_LEASE_TTL_MS'],
        leaderElectionHeartbeatMs: process.env['LEADER_ELECTION_HEARTBEAT_MS'],
        sseHeartbeatIntervalMs: process.env['SSE_HEARTBEAT_INTERVAL_MS'],
        sseMaxConnectionsPerOrder: process.env['SSE_MAX_CONNECTIONS_PER_ORDER'],
        sseMaxTotalConnections: process.env['SSE_MAX_TOTAL_CONNECTIONS'],
        sseBackpressureHighWaterMark: process.env['SSE_BACKPRESSURE_HIGH_WATER_MARK'],
        sseBackpressureCriticalMark: process.env['SSE_BACKPRESSURE_CRITICAL_MARK'],
        sseClientRetryMs: process.env['SSE_CLIENT_RETRY_MS'],
        metricsEnabled: process.env['METRICS_ENABLED'],
        metricsPath: process.env['METRICS_PATH'],
        consumerMetricsPort: process.env['CONSUMER_METRICS_PORT'],
        shutdownTimeoutMs: process.env['SHUTDOWN_TIMEOUT_MS'],
    });

    // Fail loud and clear at startup
    if (!result.success) {
        console.error('❌ Invalid configuration');
        console.error(z.treeifyError(result.error));
        process.exit(1);
    }

    return result.data;
}

// Singleton — loaded once at startup
export const config = loadConfig();