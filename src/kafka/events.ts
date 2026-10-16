// ============================================================
// Kafka Event Types and Builders
// ============================================================

import { randomUUID } from 'crypto';

// ─────────────────────────────────────────────────
// Event Envelope — Common structure for ALL events
// ─────────────────────────────────────────────────
export interface OrderEvent<T = unknown> {
    eventId: string;
    eventType: OrderEventType;
    aggregateId: string;
    timestamp: string;
    version: number;
    source: string;
    payload: T;
}

export const ORDER_EVENT_TYPES = {
    ORDER_CREATED: 'ORDER_CREATED',
    ORDER_STATUS_CHANGED: 'ORDER_STATUS_CHANGED',
    ORDER_CANCELLED: 'ORDER_CANCELLED'
} as const;

export type OrderEventType = (typeof ORDER_EVENT_TYPES)[keyof typeof ORDER_EVENT_TYPES];

// ─────────────────────────────────────────────────
// Event Payloads — Type-specific data
// ─────────────────────────────────────────────────

export interface OrderCreatedPayload {
    order: {
        id: string;
        customerId: string;
        customerName: string;
        customerEmail: string;
        customerPhone?: string;

        restaurantId: string;
        restaurantName: string;

        status: string;

        totalAmount: number;
        deliveryFee: number;
        taxAmount: number;
        grandTotal: number;

        deliveryAddress: string;
        items: Array<{
            itemName: string;
            quantity: number;
            unitPrice: number;
            totalPrice: number;
        }>;
    };
}

export interface OrderStatusChangedPayload {
    orderId: string;
    previousStatus: string;
    newStatus: string;
    changedBy: string;
    reason?: string;

    customerEmail: string;
    customerName: string;
    restaurantName: string;
}

// ─────────────────────────────────────────────────
// Event Builder Functions
// ─────────────────────────────────────────────────

export function buildOrderCreatedEvent(
    order: OrderCreatedPayload['order']
): OrderEvent<OrderCreatedPayload> {
    return {
        eventId: randomUUID(),
        eventType: ORDER_EVENT_TYPES.ORDER_CREATED,
        aggregateId: order.id,
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: { order }
    };
}

export function buildOrderStatusChangedEvent(
    data: OrderStatusChangedPayload,
): OrderEvent<OrderStatusChangedPayload> {
    return {
        eventId: randomUUID(),
        eventType: data.newStatus === 'CANCELLED'
            ? ORDER_EVENT_TYPES.ORDER_CANCELLED
            : ORDER_EVENT_TYPES.ORDER_STATUS_CHANGED,

        aggregateId: data.orderId,
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: data,
    };
}

// ─────────────────────────────────────────────────
// Serialization helpers
// ─────────────────────────────────────────────────

export function serializeEvent(event: OrderEvent): string {
    return JSON.stringify(event);
}

export function deserializeEvent(data: string): OrderEvent {
    return JSON.parse(data) as OrderEvent;
}