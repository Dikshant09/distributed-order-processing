// ============================================================
// Kafka Consumer — Reliable Event Processing
// ============================================================

import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';
import { IdempotencyStore } from './idempotency.js';
import { DlqProducer } from './dlq-producer.js';
import { withRetry } from './retry.js';
import { deserializeEvent } from './events.js';
import {
    kafkaConsumerMessagesTotal,
    kafkaConsumerProcessingDurationSeconds,
} from '../metrics/index.js';

const log = createModuleLogger('kafka-consumer');

export interface MessageHandler {
    handle(payload: EachMessagePayload): Promise<void>;
}

export class KafkaConsumer {
    private consumer: Consumer;
    private connected = false;

    constructor(
        kafka: Kafka,
        private readonly idempotencyStore: IdempotencyStore,
        private readonly dlqProducer: DlqProducer
    ) {
        this.consumer = kafka.consumer({
            groupId: config.kafkaConsumerGroupId,
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            maxWaitTimeInMs: 5000,
            retry: {
                initialRetryTime: 300,
                retries: 10,
                maxRetryTime: 30000,
            },
        });
    }

    // ─────────────────────────────────────────────────
    // Start consuming
    // ─────────────────────────────────────────────────
    async start(topic: string, handler: MessageHandler): Promise<void> {
        try {
            await this.consumer.connect();
            this.connected = true;

            log.info(
                { groupId: config.kafkaConsumerGroupId },
                '✅ Kafka consumer connected'
            );

            await this.consumer.subscribe({
                topic,
                fromBeginning: true
            });

            log.info({ topic }, `📥 Subscribed to topic: ${topic}`);

            await this.consumer.run({
                autoCommit: false,
                eachMessage: async (payload) => {
                    const { topic: msgTopic, partition, message } = payload;
                    const offset = message.offset;
                    const key = message.key?.toString();
                    const startTime = Date.now();

                    try {
                        // Extract eventId for idempotency
                        let eventId = message.headers?.['event-id']?.toString();

                        if (!eventId && message.value) {
                            try {
                                const event = deserializeEvent(message.value.toString());
                                eventId = event.eventId;
                            } catch (err) {
                                // Can't deserialize — handler will process
                            }
                        }

                        // Idempotency check
                        if (eventId) {
                            const alreadyProcessed = await this.idempotencyStore.hasBeenProcessed(eventId, config.kafkaConsumerGroupId);
                            if (alreadyProcessed) {
                                log.debug(
                                    { eventId, partition, offset },
                                    `⏭️  Event already processed — skipping (idempotent)`
                                );

                                await this.consumer.commitOffsets([
                                    {
                                        topic: msgTopic,
                                        partition,
                                        offset: (BigInt(offset) + 1n).toString(),
                                    }
                                ]);

                                kafkaConsumerMessagesTotal.inc({ topic: msgTopic, status: 'duplicate' });
                                return;
                            }
                        }

                        // Process with retry
                        log.debug(
                            { topic: msgTopic, partition, offset, key, eventId },
                            `📩 Processing message: partition=${partition} offset=${offset}`
                        );

                        const retryResult = await withRetry(
                            () => handler.handle(payload),
                            {
                                maxRetries: config.consumerMaxRetries,
                                baseDelayMs: config.consumerRetryBaseDelayMs
                            },
                            { topic: msgTopic, partition, offset, key, eventId }
                        );

                        if (retryResult.success) {
                            if (eventId) {
                                await this.idempotencyStore.markAsProcessed(eventId, config.kafkaConsumerGroupId);
                            }

                            await this.consumer.commitOffsets([
                                {
                                    topic: msgTopic,
                                    partition,
                                    offset: (BigInt(offset) + 1n).toString(),
                                },
                            ]);

                            const latency = Date.now() - startTime;
                            log.debug(
                                {
                                    topic: msgTopic, partition, offset,
                                    latencyMs: latency, attempts: retryResult.attempts,
                                },
                                `✅ Message processed and committed (${latency}ms, ${retryResult.attempts} attempt(s))`
                            );

                            kafkaConsumerMessagesTotal.inc({ topic: msgTopic, status: 'success' });
                            kafkaConsumerProcessingDurationSeconds.observe(
                                { topic: msgTopic },
                                latency / 1000
                            );

                        } else {
                            const latency = Date.now() - startTime;
                            log.error(
                                {
                                    topic: msgTopic, partition, offset, key, eventId,
                                    latencyMs: latency,
                                    error: retryResult.lastError?.message,
                                },
                                `☠️  All retries exhausted — sending to DLQ`
                            );

                            kafkaConsumerMessagesTotal.inc({ topic: msgTopic, status: 'dlq' });
                            kafkaConsumerProcessingDurationSeconds.observe(
                                { topic: msgTopic },
                                latency / 1000
                            );

                            await this.dlqProducer.sendToDlq({
                                originalTopic: msgTopic,
                                originalPartition: partition,
                                originalOffset: offset,
                                originalKey: key ?? null,
                                originalValue: message.value?.toString() ?? null,
                                errorMessage: retryResult.lastError?.message ?? 'Unknown error',
                                errorStack: retryResult.lastError?.stack,
                                retryCount: retryResult.attempts,
                                failedAt: new Date().toISOString(),
                                consumerGroup: config.kafkaConsumerGroupId
                            });

                            if (eventId) {
                                await this.idempotencyStore.markAsProcessed(eventId, config.kafkaConsumerGroupId);
                            }

                            await this.consumer.commitOffsets([
                                {
                                    topic: msgTopic,
                                    partition,
                                    offset: (BigInt(offset) + 1n).toString(),
                                },
                            ]);
                        }
                    } catch (err) {
                        const latency = Date.now() - startTime;

                        log.error(
                            {
                                err,
                                topic: msgTopic,
                                partition,
                                offset,
                                key,
                                latencyMs: latency,
                            },
                            `❌ Unexpected error in consumer pipeline — skipping`
                        );

                        await this.consumer.commitOffsets([
                            {
                                topic: msgTopic,
                                partition,
                                offset: (BigInt(offset) + 1n).toString(),
                            }
                        ]);
                    }
                }
            });
        } catch (err) {
            log.error({ err }, '❌ Failed to start Kafka consumer');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Graceful shutdown
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (!this.connected) return;

        try {
            await this.consumer.disconnect();
            this.connected = false;
            log.info('✅ Kafka consumer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Error disconnecting Kafka consumer');
        }
    }
}