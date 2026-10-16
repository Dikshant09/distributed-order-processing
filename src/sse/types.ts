// ============================================================
// SSE Types — Realtime Streaming
// ============================================================

// ─────────────────────────────────────────────────
// SSE Event — Wire payload format
// ─────────────────────────────────────────────────
export interface SSEEvent {
    /** Optional event name. Client uses addEventListener(name, ...) to filter. */
    event?: string;

    /** The payload data. Will be JSON.stringify'd if object. */
    data: string | Record<string, unknown>;

    /** Event ID. Client sends Last-Event-ID on reconnect for resume. */
    id?: string;

    /** Suggested reconnection delay in milliseconds. Sent once on initial connect. */
    retry?: number;
}

// ─────────────────────────────────────────────────
// Connection State Machine
// ─────────────────────────────────────────────────
export enum SSEConnectionState {
    OPEN = 'OPEN',
    DRAINING = 'DRAINING',
    CLOSED = 'CLOSED',
}

// ─────────────────────────────────────────────────
// Backpressure Configuration
// ─────────────────────────────────────────────────
export interface BackpressureConfig {
    /** Buffer size that triggers a WARNING log. Default: 64KB */
    highWaterMark: number;

    /** Buffer size that triggers the backpressure POLICY. Default: 256KB */
    criticalMark: number;

    /** What to do when critical mark is exceeded */
    policy: BackpressurePolicy;
}

// ─────────────────────────────────────────────────
// Backpressure Policy
// ─────────────────────────────────────────────────
export enum BackpressurePolicy {
    /** Drop the oldest buffered event (keep the newest) */
    DROP_OLDEST = 'DROP_OLDEST',

    /** Drop the newest event (keep existing buffer) */
    DROP_NEWEST = 'DROP_NEWEST',

    /** Disconnect the slow client entirely */
    DISCONNECT = 'DISCONNECT',
}

// ─────────────────────────────────────────────────
// Order Stream Events — Domain-specific SSE payloads
// ─────────────────────────────────────────────────
export interface OrderStreamEvent {
    /** The event type for SSE routing */
    type: OrderStreamEventType;

    /** The order ID this event belongs to */
    orderId: string;

    /** Event-specific payload */
    payload: Record<string, unknown>;

    /** ISO 8601 timestamp */
    timestamp: string;
}

export type OrderStreamEventType =
    | 'status_update'
    | 'order_assigned'
    | 'delivery_update'
    | 'order_cancelled';

// ─────────────────────────────────────────────────
// SSE Manager Metrics
// ─────────────────────────────────────────────────
export interface SSEMetrics {
    /** Currently active SSE connections */
    activeConnections: number;

    /** Number of distinct orders being tracked */
    activeOrders: number;

    /** Total connections opened since startup */
    totalConnectionsOpened: number;

    /** Total connections closed since startup */
    totalConnectionsClosed: number;

    /** Number of backpressure events triggered */
    backpressureEventsTotal: number;

    /** Number of clients disconnected due to backpressure */
    backpressureDisconnectsTotal: number;

    /** Total events sent across all connections */
    eventsSentTotal: number;

    /** Total events dropped due to backpressure */
    eventsDroppedTotal: number;
}

// ─────────────────────────────────────────────────
// Redis Pub/Sub Message Format
// ─────────────────────────────────────────────────
export interface RedisPubSubMessage {
    /** The SSE event type to use */
    eventType: OrderStreamEventType;

    /** The order this message belongs to */
    orderId: string;

    /** Event-specific data to send to the client */
    data: Record<string, unknown>;

    /** ISO 8601 timestamp of the original event */
    timestamp: string;

    /** Source system that generated this event */
    source: string;
}