// ============================================================
// Kafka Producer — Publishing Events
// ============================================================

import { Kafka, Producer, ProducerRecord } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';
import {
    OrderEvent,
    OrderCreatedPayload,
    OrderStatusChangedPayload,
    buildOrderCreatedEvent,
    buildOrderStatusChangedEvent,
    serializeEvent,
} from './events.js';

const log = createModuleLogger('kafka-producer');

export class EventProducer {
    private producer: Producer;
    private connected = false;

    constructor(kafka: Kafka) {
        this.producer = kafka.producer({
            allowAutoTopicCreation: false,
            transactionTimeout: 30000,
        });
    }

    // ─────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────

    async connect(): Promise<void> {
        try {
            await this.producer.connect();
            this.connected = true;
            log.info('✅ Kafka producer connected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect Kafka producer');
        }
    }

    async disconnect(): Promise<void> {
        try {
            await this.producer.disconnect();
            this.connected = false;
            log.info('✅ Kafka producer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Failed to disconnect Kafka producer');
        }
    }

    // ─────────────────────────────────────────────────
    // Publish events
    // ─────────────────────────────────────────────────

    async publishOrderCreated(order: OrderCreatedPayload['order']): Promise<void> {
        const event = buildOrderCreatedEvent(order);
        await this.publishEvent(event);
    }

    async publishOrderStatusChanged(data: OrderStatusChangedPayload): Promise<void> {
        const event = buildOrderStatusChangedEvent(data);
        await this.publishEvent(event);
    }

    // ─────────────────────────────────────────────────
    // Core publish method
    // ─────────────────────────────────────────────────
    private async publishEvent(event: OrderEvent): Promise<void> {
        if (!this.connected) {
            log.warn(
                { eventType: event.eventType, aggregateId: event.aggregateId },
                '⚠️  Kafka producer not connected — event will NOT be published'
            );
        }

        const topic = config.kafkaTopicOrderEvents;

        const record: ProducerRecord = {
            topic,
            messages: [
                {
                    key: event.aggregateId,
                    value: serializeEvent(event),
                    headers: {
                        'event-type': event.eventType,
                        'event-id': event.eventId,
                        'source': event.source,
                    }
                }
            ],
            acks: -1
        };

        const startTime = Date.now();

        try {
            const result = await this.producer.send(record);
            const latency = Date.now() - startTime;
            log.info(
                {
                    eventType: event.eventType,
                    eventId: event.eventId,
                    aggregateId: event.aggregateId,
                    topic,
                    partition: result[0]?.partition,
                    offset: result[0]?.offset,
                    latencyMs: latency,
                },
                `📤 Event published: ${event.eventType} (${latency}ms)`
            );
        } catch (err) {
            const latency = Date.now() - startTime;
            log.error(
                {
                    err,
                    eventType: event.eventType,
                    eventId: event.eventId,
                    aggregateId: event.aggregateId,
                    latencyMs: latency,
                },
                `❌ Failed to publish event: ${event.eventType}`
            );
        }
    }
}