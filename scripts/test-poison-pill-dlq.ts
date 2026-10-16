// ============================================================
// Test Suite: Poison Pill DLQ, Outbox Locking & Consumer Idempotency
// ============================================================

import test from 'node:test';
import assert from 'node:assert/strict';
import { Pool, PoolClient } from 'pg';
import { NotificationConsumerHandler } from '../src/consumers/notification-consumer.js';
import { DlqProducer, DlqMessage } from '../src/kafka/dlq-producer.js';
import { IdempotencyStore, DuplicateEventError } from '../src/kafka/idempotency.js';
import { OutboxRepository } from '../src/outbox/outbox.repository.js';
import { config } from '../src/config/index.js';
import { ORDER_EVENT_TYPES, OrderEvent, OrderCreatedPayload } from '../src/kafka/events.js';

// ─────────────────────────────────────────────────
// Mock Helpers
// ─────────────────────────────────────────────────
class MockDlqProducer {
    public sentMessages: DlqMessage[] = [];

    async sendToDlq(message: DlqMessage): Promise<void> {
        this.sentMessages.push(message);
    }
}

function createMockPool() {
    const executedQueries: Array<{ sql: string; params?: unknown[] }> = [];

    const mockClient = {
        query: async (sql: string, params?: unknown[]) => {
            executedQueries.push({ sql, params });
            if (sql.includes('RETURNING *') && sql.includes('notifications')) {
                return {
                    rows: [{
                        id: 'notif-1',
                        order_id: params?.[0],
                        type: params?.[1],
                        channel: params?.[2],
                        status: 'PENDING',
                        recipient: params?.[3],
                        subject: params?.[4],
                        content: params?.[5],
                        retry_count: 0,
                        failure_reason: null,
                        sent_at: null,
                        created_at: new Date()
                    }],
                    rowCount: 1
                };
            }
            if (sql.includes('INSERT INTO processed_events')) {
                return { rows: [{ event_id: params?.[0] }], rowCount: 1 };
            }
            return { rows: [], rowCount: 1 };
        },
        release: () => {}
    } as unknown as PoolClient;

    const mockPool = {
        query: async (sql: string, params?: unknown[]) => {
            executedQueries.push({ sql, params });
            return { rows: [], rowCount: 0 };
        },
        connect: async () => mockClient,
        executedQueries
    } as unknown as Pool & { executedQueries: Array<{ sql: string; params?: unknown[] }> };

    return { mockPool, mockClient, executedQueries };
}

// ─────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────
test('1. Poison Pill (Malformed JSON) is captured and routed to DLQ without dropping', async () => {
    const { mockPool, executedQueries } = createMockPool();
    const mockDlq = new MockDlqProducer();
    const idempotencyStore = new IdempotencyStore(mockPool);

    const handler = new NotificationConsumerHandler(
        mockPool,
        null,
        'op:',
        mockDlq as unknown as DlqProducer,
        idempotencyStore
    );

    const malformedPayload = {
        topic: 'order-events',
        partition: 1,
        message: {
            offset: '105',
            key: Buffer.from('order-uuid-999'),
            value: Buffer.from('{ malformed_json_not_valid_payload !!!'),
            attributes: 0,
            timestamp: String(Date.now()),
            headers: {}
        },
        heartbeat: async () => {},
        pause: () => () => {}
    };

    // Execute handler — should handle poison pill gracefully without throwing
    await handler.handle(malformedPayload);

    // Verify sent to DLQ with exact fields
    assert.equal(mockDlq.sentMessages.length, 1, 'Expected exactly 1 DLQ message sent');
    const dlqMsg = mockDlq.sentMessages[0]!;
    assert.equal(dlqMsg.originalTopic, 'order-events');
    assert.equal(dlqMsg.originalPartition, 1);
    assert.equal(dlqMsg.originalOffset, '105');
    assert.equal(dlqMsg.originalKey, 'order-uuid-999');
    assert.equal(dlqMsg.originalValue, '{ malformed_json_not_valid_payload !!!');
    assert.equal(dlqMsg.errorMessage, 'Deserialization failed');
    assert.equal(dlqMsg.retryCount, 0);
    assert.equal(dlqMsg.consumerGroup, config.kafkaConsumerGroupId);
    assert.ok(dlqMsg.errorStack, 'Expected error stack to be present in DLQ message');

    // Verify no notifications or processed_events were written for poison pill
    const notificationInserts = executedQueries.filter(q => q.sql.includes('INSERT INTO notifications'));
    assert.equal(notificationInserts.length, 0, 'No notification row should be inserted for poison pill');
});

test('2. Poison pill handling allows consumer offset advancement', async () => {
    const { mockPool } = createMockPool();
    const mockDlq = new MockDlqProducer();
    const handler = new NotificationConsumerHandler(
        mockPool,
        null,
        'op:',
        mockDlq as unknown as DlqProducer
    );

    let committedOffset: string | null = null;
    const offset = '50';
    const fakeConsumer = {
        commitOffsets: async (offsets: Array<{ topic: string; partition: number; offset: string }>) => {
            committedOffset = offsets[0]?.offset ?? null;
        }
    };

    const payload = {
        topic: 'order-events',
        partition: 0,
        message: {
            offset,
            key: Buffer.from('key-1'),
            value: Buffer.from('corrupted::bytes'),
            attributes: 0,
            timestamp: String(Date.now()),
            headers: {}
        },
        heartbeat: async () => {},
        pause: () => () => {}
    };

    // Handler processes poison pill
    await handler.handle(payload);
    // Simulate what KafkaConsumer does upon handler success
    await fakeConsumer.commitOffsets([
        {
            topic: payload.topic,
            partition: payload.partition,
            offset: (BigInt(offset) + 1n).toString()
        }
    ]);

    assert.equal(committedOffset, '51', 'Consumer should commit next offset (51) to advance past poison pill');
    assert.equal(mockDlq.sentMessages.length, 1);
});

test('3. Consumer idempotency and business write share the same transaction boundary', async () => {
    const { mockPool, executedQueries } = createMockPool();
    const mockDlq = new MockDlqProducer();
    const idempotencyStore = new IdempotencyStore(mockPool);

    const handler = new NotificationConsumerHandler(
        mockPool,
        null,
        'op:',
        mockDlq as unknown as DlqProducer,
        idempotencyStore
    );

    const validEvent: OrderEvent<OrderCreatedPayload> = {
        eventId: 'evt-unique-123',
        eventType: ORDER_EVENT_TYPES.ORDER_CREATED,
        aggregateId: 'ord-456',
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: {
            order: {
                id: 'ord-456',
                customerId: 'cust-1',
                customerName: 'Alice Doe',
                customerEmail: 'alice@example.com',
                restaurantId: 'rest-1',
                restaurantName: 'Pizza Heaven',
                status: 'PENDING',
                totalAmount: 25.00,
                deliveryFee: 3.99,
                taxAmount: 2.00,
                grandTotal: 30.99,
                deliveryAddress: '123 Main St',
                items: [
                    { itemName: 'Margherita Pizza', quantity: 1, unitPrice: 25.00, totalPrice: 25.00 }
                ]
            }
        }
    };

    const payload = {
        topic: 'order-events',
        partition: 0,
        message: {
            offset: '10',
            key: Buffer.from('ord-456'),
            value: Buffer.from(JSON.stringify(validEvent)),
            attributes: 0,
            timestamp: String(Date.now()),
            headers: { 'event-id': Buffer.from('evt-unique-123') }
        },
        heartbeat: async () => {},
        pause: () => () => {}
    };

    await handler.handle(payload);

    // Verify transaction sequence on the client
    const sqlStatements = executedQueries.map(q => q.sql.trim());
    const beginIdx = sqlStatements.findIndex(s => s === 'BEGIN');
    const notifInsertIdx = sqlStatements.findIndex(s => s.includes('INSERT INTO notifications'));
    const processedInsertIdx = sqlStatements.findIndex(s => s.includes('INSERT INTO processed_events'));
    const commitIdx = sqlStatements.findIndex(s => s === 'COMMIT');

    assert.ok(beginIdx !== -1, 'Expected BEGIN transaction statement');
    assert.ok(notifInsertIdx > beginIdx, 'notifications INSERT must occur after BEGIN');
    assert.ok(processedInsertIdx > notifInsertIdx, 'processed_events INSERT must occur in same transaction');
    assert.ok(commitIdx > processedInsertIdx, 'COMMIT must occur after both inserts');
});

test('4. Concurrent duplicate event triggers rollback and skips without duplicate notifications', async () => {
    const executedQueries: string[] = [];

    // Mock client where processed_events returns rowCount 0 (simulating duplicate race)
    const mockClient = {
        query: async (sql: string) => {
            executedQueries.push(sql.trim());
            if (sql.includes('INSERT INTO notifications')) {
                return {
                    rows: [{
                        id: 'notif-2',
                        order_id: 'ord-789',
                        type: 'ORDER_CONFIRMED',
                        channel: 'EMAIL',
                        status: 'PENDING',
                        recipient: 'bob@example.com',
                        subject: 'Order Confirmed',
                        content: 'Your order has been confirmed',
                        retry_count: 0,
                        created_at: new Date()
                    }],
                    rowCount: 1
                };
            }
            if (sql.includes('INSERT INTO processed_events')) {
                // Duplicate conflict! Row count = 0
                return { rows: [], rowCount: 0 };
            }
            return { rows: [], rowCount: 1 };
        },
        release: () => {}
    } as unknown as PoolClient;

    const mockPool = {
        query: async () => ({ rows: [], rowCount: 0 }),
        connect: async () => mockClient
    } as unknown as Pool;

    const idempotencyStore = new IdempotencyStore(mockPool);
    const handler = new NotificationConsumerHandler(
        mockPool,
        null,
        'op:',
        null,
        idempotencyStore
    );

    const event: OrderEvent<OrderCreatedPayload> = {
        eventId: 'evt-duplicate-999',
        eventType: ORDER_EVENT_TYPES.ORDER_CREATED,
        aggregateId: 'ord-789',
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: {
            order: {
                id: 'ord-789',
                customerId: 'cust-2',
                customerName: 'Bob Smith',
                customerEmail: 'bob@example.com',
                restaurantId: 'rest-2',
                restaurantName: 'Burger Joint',
                status: 'PENDING',
                totalAmount: 15.00,
                deliveryFee: 2.00,
                taxAmount: 1.20,
                grandTotal: 18.20,
                deliveryAddress: '456 Oak St',
                items: []
            }
        }
    };

    const payload = {
        topic: 'order-events',
        partition: 0,
        message: {
            offset: '20',
            key: Buffer.from('ord-789'),
            value: Buffer.from(JSON.stringify(event)),
            attributes: 0,
            timestamp: String(Date.now()),
            headers: {}
        },
        heartbeat: async () => {},
        pause: () => () => {}
    };

    // Handler should handle DuplicateEventError cleanly
    await handler.handle(payload);

    // Verify that ROLLBACK was called due to DuplicateEventError
    assert.ok(executedQueries.includes('ROLLBACK'), 'Expected transaction to ROLLBACK when duplicate detected');
    assert.ok(!executedQueries.includes('COMMIT'), 'Transaction must NOT COMMIT duplicate');
});

test('5. OutboxRepository claims unpublished events atomically using UPDATE ... RETURNING', async () => {
    let capturedQuery = '';
    let capturedParams: unknown[] = [];

    const mockPool = {
        query: async (sql: string, params?: unknown[]) => {
            capturedQuery = sql;
            capturedParams = params ?? [];
            return {
                rows: [{
                    id: 'outbox-1',
                    aggregate_id: 'ord-100',
                    event_type: 'ORDER_CREATED',
                    topic: 'order-events',
                    key: 'ord-100',
                    payload: {},
                    headers: {},
                    status: 'PROCESSING',
                    locked_at: new Date(),
                    created_at: new Date(),
                    published_at: null
                }]
            };
        }
    } as unknown as Pool;

    const repo = new OutboxRepository(mockPool);
    const events = await repo.fetchUnpublished(10);

    assert.equal(events.length, 1);
    assert.equal(events[0]?.status, 'PROCESSING');
    assert.ok(capturedQuery.includes("UPDATE outbox_events"), 'Must use UPDATE for atomic status claim');
    assert.ok(capturedQuery.includes("SET status = 'PROCESSING', locked_at = NOW()"), 'Must set PROCESSING and locked_at');
    assert.ok(capturedQuery.includes("FOR UPDATE SKIP LOCKED"), 'Must use FOR UPDATE SKIP LOCKED');
    assert.ok(capturedQuery.includes("INTERVAL '30 seconds'"), 'Must include 30-second reclaim timeout for crashed workers');
    assert.ok(capturedQuery.includes("RETURNING *"), 'Must use RETURNING to return claimed rows');
    assert.deepEqual(capturedParams, [10]);
});
