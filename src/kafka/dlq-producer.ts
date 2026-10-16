// ============================================================
// Dead Letter Queue (DLQ) Producer
// ============================================================

import { Kafka, Producer } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';

const log = createModuleLogger('dlq-producer');

export interface DlqMessage {
    originalTopic: string;
    originalPartition: number;
    originalOffset: string;
    originalKey: string | null;
    originalValue: string | null;
    errorMessage: string;
    errorStack?: string;
    retryCount: number;
    failedAt: string;
    consumerGroup: string;
}

export class DlqProducer {
    private producer: Producer;
    private connected = false;

    constructor(kafka: Kafka) {
        this.producer = kafka.producer({
            allowAutoTopicCreation: false,
        });
    }

    async connect(): Promise<void> {
        try {
            await this.producer.connect();
            this.connected = true;
            log.info({ topic: config.kafkaTopicDlq }, '✅ DLQ producer connected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect DLQ producer');
        }
    }

    async disconnect(): Promise<void> {
        if (!this.connected) return;
        try {
            await this.producer.disconnect();
            this.connected = false;
            log.info('✅ DLQ producer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect DLQ producer');
        }
    }

    // ─────────────────────────────────────────────────
    // Send a failed event to the DLQ
    // ─────────────────────────────────────────────────
    async sendToDlq(dlqMessage: DlqMessage): Promise<void> {
        if (!this.connected) {
            log.warn(
                { originalTopic: dlqMessage.originalTopic },
                '⚠️  DLQ producer not connected — failed event will be lost'
            );
            return;
        }

        try {
            await this.producer.send({
                topic: config.kafkaTopicDlq,
                messages: [
                    {
                        key: dlqMessage.originalKey ?? undefined,
                        value: dlqMessage.originalValue ?? '',
                        headers: {
                            'dlq-original-topic': dlqMessage.originalTopic,
                            'dlq-original-partition': String(dlqMessage.originalPartition),
                            'dlq-original-offset': dlqMessage.originalOffset,
                            'dlq-error-message': dlqMessage.errorMessage,
                            'dlq-error-stack': dlqMessage.errorStack ?? '',
                            'dlq-retry-count': String(dlqMessage.retryCount),
                            'dlq-failed-at': dlqMessage.failedAt,
                            'dlq-consumer-group': dlqMessage.consumerGroup,
                        },
                    }
                ],
                acks: -1,
            });

            log.info({
                originalTopic: dlqMessage.originalTopic,
                originalPartition: dlqMessage.originalPartition,
                originalOffset: dlqMessage.originalOffset,
                retryCount: dlqMessage.retryCount,
                errorMessage: dlqMessage.errorMessage,
            },
                '☠️  Event sent to DLQ'
            );
        } catch (err) {
            log.error(
                {
                    err,
                    originalTopic: dlqMessage.originalTopic,
                    originalOffset: dlqMessage.originalOffset
                },
                '❌ CRITICAL: Failed to send event to DLQ'
            );
        }
    }
}