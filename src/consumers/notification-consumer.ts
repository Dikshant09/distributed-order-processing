// ============================================================
// Notification Consumer — Event-Driven Notification Processing
// ============================================================

import { EachMessagePayload } from "kafkajs";
import { Pool } from 'pg';
import { MessageHandler } from "../kafka/consumer";
import {
    OrderEvent,
    OrderCreatedPayload,
    OrderStatusChangedPayload,
    ORDER_EVENT_TYPES,
    deserializeEvent
} from '../kafka/events.js';
import { config } from '../config/index.js';
import { DlqProducer } from '../kafka/dlq-producer.js';
import { IdempotencyStore, DuplicateEventError } from '../kafka/idempotency.js';
import { NotificationService } from "../modules/notifications/notification.service";
import { NotificationRepository } from "../modules/notifications/notification.repository";
import { createModuleLogger } from "../shared/logger";
import Redis from 'ioredis';
import { publishOrderUpdate } from "../sse/redis-subscriber.js";
import type { RedisPubSubMessage, OrderStreamEventType } from "../sse/types.js";

const log = createModuleLogger('notification-consumer');

export class NotificationConsumerHandler implements MessageHandler {
    private readonly notificationService: NotificationService;
    private readonly dlqProducer: DlqProducer | null;
    private readonly idempotencyStore: IdempotencyStore;

    constructor(
        pool: Pool,
        private readonly redis: Redis | null = null,
        private readonly redisKeyPrefix: string = 'op:',
        dlqProducer: DlqProducer | null = null,
        idempotencyStore: IdempotencyStore | null = null,
    ) {
        const notificationRepo = new NotificationRepository(pool);
        this.notificationService = new NotificationService(notificationRepo);
        this.dlqProducer = dlqProducer;
        this.idempotencyStore = idempotencyStore ?? new IdempotencyStore(pool);
    }

    // ─────────────────────────────────────────────────
    // Handle incoming Kafka messages
    // ─────────────────────────────────────────────────
    async handle(payload: EachMessagePayload): Promise<void> {
        const { message, partition } = payload;
        const value = message.value?.toString();

        if (!value) {
            log.warn({ partition, offset: message.offset }, 'Empty message — skipping');
            return;
        }

        // Deserialize the event envelope
        let event: OrderEvent;
        try {
            event = deserializeEvent(value);
        } catch (err) {
            log.error(
                { err, partition, offset: message.offset, rawValue: value.slice(0, 200) },
                '❌ Failed to deserialize event — sending to DLQ (poison pill)'
            );

            if (this.dlqProducer) {
                await this.dlqProducer.sendToDlq({
                    originalTopic: payload.topic,
                    originalPartition: partition,
                    originalOffset: message.offset,
                    originalKey: message.key?.toString() ?? null,
                    originalValue: value,
                    errorMessage: "Deserialization failed",
                    errorStack: err instanceof Error ? err.stack : undefined,
                    retryCount: 0,
                    failedAt: new Date().toISOString(),
                    consumerGroup: config.kafkaConsumerGroupId,
                });
            } else {
                log.warn(
                    { partition, offset: message.offset },
                    '⚠️  DLQ producer not available in handler — poison pill dropped'
                );
            }
            return;
        }

        log.info(
            {
                eventType: event.eventType,
                eventId: event.eventId,
                aggregateId: event.aggregateId,
                partition,
                offset: message.offset,
            },
            `📨 Received event: ${event.eventType} for order ${event.aggregateId}`
        );

        // Route by event type
        try {
            switch (event.eventType) {
                case ORDER_EVENT_TYPES.ORDER_CREATED:
                    await this.handleOrderCreated(event as OrderEvent<OrderCreatedPayload>);
                    break;
                case ORDER_EVENT_TYPES.ORDER_STATUS_CHANGED:
                case ORDER_EVENT_TYPES.ORDER_CANCELLED:
                    await this.handleOrderStatusChanged(event as OrderEvent<OrderStatusChangedPayload>);
                    break;
                default: {
                    const unknownType = event.eventType as string;
                    log.warn(
                        { eventType: unknownType, eventId: event.eventId },
                        `⚠️  Unknown event type: ${unknownType} — skipping`
                    );
                }
            }
        } catch (err) {
            if (err instanceof DuplicateEventError) {
                log.info(
                    { eventId: event.eventId, partition, offset: message.offset },
                    `⏭️  Event already processed concurrently — skipping duplicate (idempotent)`
                );
                return;
            }
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Handle ORDER_CREATED
    // ─────────────────────────────────────────────────
    private async handleOrderCreated(
        event: OrderEvent<OrderCreatedPayload>
    ): Promise<void> {
        const { order } = event.payload;
        const startTime = Date.now();

        const { subject, content } = this.notificationService.buildOrderConfirmationContent({
            id: order.id,
            customerName: order.customerName,
            restaurantName: order.restaurantName,
            grandTotal: order.grandTotal,
        });

        await this.notificationService.send(
            {
                orderId: order.id,
                type: 'ORDER_CONFIRMED',
                channel: 'EMAIL',
                recipient: order.customerEmail,
                subject,
                content
            },
            async (client) => {
                const inserted = await this.idempotencyStore.markAsProcessed(
                    event.eventId,
                    config.kafkaConsumerGroupId,
                    client
                );
                if (!inserted) {
                    throw new DuplicateEventError(event.eventId);
                }
            }
        );

        const latency = Date.now() - startTime;
        log.info(
            {
                orderId: order.id,
                eventId: event.eventId,
                latencyMs: latency,
            },
            `✉️  Order confirmation sent for ${order.id} (${latency}ms)`
        );

        // Publish to Redis Pub/Sub for SSE fanout
        await this.publishToSSE(order.id, 'status_update', {
            orderId: order.id,
            status: 'PENDING',
            customerName: order.customerName,
            restaurantName: order.restaurantName,
            grandTotal: order.grandTotal,
            message: 'Order placed successfully',
        });
    }

    // ─────────────────────────────────────────────────
    // Handle ORDER_STATUS_CHANGED / ORDER_CANCELLED
    // ─────────────────────────────────────────────────
    private async handleOrderStatusChanged(
        event: OrderEvent<OrderStatusChangedPayload>
    ): Promise<void> {
        const { orderId, customerEmail, customerName, newStatus } = event.payload;
        const startTime = Date.now();

        const { subject, content } = this.notificationService.buildOrderUpdateContent({
            id: orderId,
            customerName,
            status: newStatus,
        });

        await this.notificationService.send(
            {
                orderId,
                type: newStatus === 'CANCELLED' ? 'ORDER_CANCELLED' : 'ORDER_STATUS_CHANGED',
                channel: 'EMAIL',
                recipient: customerEmail,
                subject,
                content,
            },
            async (client) => {
                const inserted = await this.idempotencyStore.markAsProcessed(
                    event.eventId,
                    config.kafkaConsumerGroupId,
                    client
                );
                if (!inserted) {
                    throw new DuplicateEventError(event.eventId);
                }
            }
        );

        const latency = Date.now() - startTime;
        log.info(
            {
                orderId,
                eventId: event.eventId,
                newStatus,
                latencyMs: latency,
            },
            `✉️  Status notification sent: ${newStatus} for ${orderId} (${latency}ms)`
        );

        // Publish status change to SSE
        const sseEventType: OrderStreamEventType =
            newStatus === 'CANCELLED' ? 'order_cancelled' : 'status_update';

        await this.publishToSSE(orderId, sseEventType, {
            orderId,
            previousStatus: event.payload.previousStatus,
            newStatus,
            changedBy: event.payload.changedBy,
            reason: event.payload.reason,
            customerName,
            restaurantName: event.payload.restaurantName,
        });
    }

    // ─────────────────────────────────────────────────
    // Publish to Redis Pub/Sub for SSE fanout
    // ─────────────────────────────────────────────────
    private async publishToSSE(
        orderId: string,
        eventType: OrderStreamEventType,
        data: Record<string, unknown>
    ): Promise<void> {
        if (!this.redis) return;

        const message: RedisPubSubMessage = {
            eventType,
            orderId,
            data,
            timestamp: new Date().toISOString(),
            source: 'notification-consumer',
        };

        await publishOrderUpdate(this.redis, this.redisKeyPrefix, message);
    }
}