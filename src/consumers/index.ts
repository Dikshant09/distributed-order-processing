// ============================================================
// Consumer Process Entry Point
// ============================================================

import { Pool } from 'pg';
import Redis from 'ioredis';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';
import { createPool } from '../db/pool';
import { createKafkaClient } from '../kafka/client';
import { KafkaConsumer } from '../kafka/consumer';
import { IdempotencyStore } from '../kafka/idempotency.js';
import { DlqProducer } from '../kafka/dlq-producer.js';
import { NotificationConsumerHandler } from './notification-consumer';
import { collectDefaultMetrics, register } from '../metrics/metrics.js';
import { createServer } from 'node:http';

const log = createModuleLogger('consumer-main');

async function main(): Promise<void> {
    log.info(
        {
            service: 'notification-consumer',
            nodeEnv: config.nodeEnv,
            kafkaBrokers: config.kafkaBrokers,
            consumerGroup: config.kafkaConsumerGroupId,
            topic: config.kafkaTopicOrderEvents,
            dlqTopic: config.kafkaTopicDlq,
            maxRetries: config.consumerMaxRetries,
            retryBaseDelayMs: config.consumerRetryBaseDelayMs
        },
        '🚀 Starting notification consumer...'
    );

    // Step 0: Initialize Prometheus metrics collection
    if (config.metricsEnabled) {
        collectDefaultMetrics();
        log.info('📊 Prometheus metrics collection initialized');
    }

    // Step 1: Connect to PostgreSQL
    const pool: Pool = createPool();

    try {
        const client = await pool.connect();
        client.release();
        log.info('✅ PostgreSQL connected');
    } catch (err) {
        log.fatal({ err }, '❌ Failed to connect to PostgreSQL');
        process.exit(1);
    }

    // Step 2: Create idempotency store
    const idempotencyStore = new IdempotencyStore(pool);
    log.info('✅ Idempotency store initialized');

    // Step 3: Create and connect DLQ producer
    const kafka = createKafkaClient();
    const dlqProducer = new DlqProducer(kafka);
    await dlqProducer.connect();

    // Step 4: Create Kafka consumer
    const consumer = new KafkaConsumer(kafka, idempotencyStore, dlqProducer);

    // Step 5: Connect Redis for SSE Pub/Sub
    let redis: Redis | null = null;

    if (config.redisUrl) {
        try {
            redis = new Redis(config.redisUrl, {
                maxRetriesPerRequest: 3,
                connectTimeout: 5000,
                retryStrategy(times: number) {
                    const delay = Math.min(times * 100, 10000);
                    if (times <= 5 || times % 10 === 0) {
                        console.warn(`[consumer] Redis reconnecting (attempt ${times}), delay ${delay}ms`);
                    }
                    return delay;
                }
            });
            await redis.ping();
            log.info('✅ Redis connected (for SSE Pub/Sub publishing)');
        } catch (err) {
            log.warn(
                { err },
                '⚠️  Redis connection failed — SSE updates will be skipped (degraded mode)'
            );
            redis = null;
        }
    } else {
        log.info('ℹ️  Redis not configured — SSE Pub/Sub disabled');
    }

    // Pass redis, dlqProducer, and idempotencyStore to the handler
    const handler = new NotificationConsumerHandler(
        pool,
        redis,
        config.redisKeyPrefix,
        dlqProducer,
        idempotencyStore
    );

    // Step 6: Graceful shutdown
    const shutdown = async (signal: string) => {
        log.info({ signal }, '🛑 Shutting down notification consumer...');

        try {
            await consumer.stop();
            log.info('  ↳ Kafka consumer stopped');

            await dlqProducer.disconnect();
            log.info('  ↳ DLQ producer stopped');

            if (redis) {
                await redis.quit();
                log.info('  ↳ Redis disconnected');
            }

            await pool.end();
            log.info('  ↳ PostgreSQL pool closed');

            log.info('✅ Notification consumer shut down cleanly');
            process.exit(0);
        } catch (err) {
            log.error({ err }, '❌ Error during shutdown');
            process.exit(1);
        }
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    // Step 7: Start consuming
    try {
        await consumer.start(config.kafkaTopicOrderEvents, handler);
        log.info('📥 Notification consumer is running');

        // Step 8: Start metrics HTTP server
        if (config.metricsEnabled) {
            const metricsServer = createServer(async (req, res) => {
                if (req.url === '/metrics' && req.method === 'GET') {
                    res.writeHead(200, { 'Content-Type': register.contentType });
                    res.end(await register.metrics());
                } else {
                    res.writeHead(404);
                    res.end('Not found');
                }
            });

            metricsServer.listen(config.consumerMetricsPort, () => {
                log.info(
                    { port: config.consumerMetricsPort },
                    `📊 Consumer metrics server listening on port ${config.consumerMetricsPort}`
                );
            });
        }

    } catch (err) {
        log.fatal({ err }, '❌ Failed to start notification consumer');
        await dlqProducer.disconnect();
        await pool.end();
        process.exit(1);
    }
}

process.on('unhandledRejection', (reason) => {
    log.fatal({ err: reason }, '❌ Unhandled rejection in consumer');
    process.exit(1);
});

process.on('uncaughtException', (err) => {
    log.fatal({ err }, '❌ Uncaught exception in consumer');
    process.exit(1);
});

main().catch((err) => {
    log.fatal({ err }, '❌ Consumer startup failed');
    process.exit(1);
});