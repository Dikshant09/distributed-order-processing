// ============================================================
// SSE Manager — Connection Registry & Fanout
// ============================================================

import { ServerResponse } from "http";
import { createModuleLogger } from "../shared/logger";
import { SSEConnection } from "./sse-connection";
import {
    SSEEvent,
    SSEMetrics,
    SSEConnectionState,
    BackpressureConfig
} from './types.js';
import {
    sseConnectionsActive,
    sseEventsSentTotal,
    sseBackpressureEventsTotal
} from '../metrics/index.js';

const log = createModuleLogger('sse-manager');

export interface SSEManagerConfig {
    maxTotalConnections: number;
    maxConnectionsPerOrder: number;
    heartbeatIntervalMs: number;
    clientRetryMs: number;
    backpressureConfig: BackpressureConfig;
}

export class SSEManager {
    private readonly connections = new Map<string, Set<SSEConnection>>();

    // Metrics
    private _totalConnectionsOpened = 0;
    private _totalConnectionsClosed = 0;
    private _backpressureDisconnects = 0;
    private _eventsSentTotal = 0;
    private _eventsDroppedTotal = 0;

    // Callbacks for external subscriber coordination
    private onFirstConnection: ((orderId: string) => void) | null = null;
    private onLastDisconnection: ((orderId: string) => void) | null = null;

    constructor(private readonly config: SSEManagerConfig) {
        log.info(
            {
                maxTotalConnections: config.maxTotalConnections,
                maxConnectionsPerOrder: config.maxConnectionsPerOrder,
                heartbeatIntervalMs: config.heartbeatIntervalMs,
            },
            '📡 SSE Manager initialized'
        );
    }

    // ─────────────────────────────────────────────────
    // Set external event handlers
    // ─────────────────────────────────────────────────
    setConnectionHandlers(
        onFirst: (orderId: string) => void,
        onLast: (orderId: string) => void,
    ): void {
        this.onFirstConnection = onFirst;
        this.onLastDisconnection = onLast;
    }

    // ─────────────────────────────────────────────────
    // Register a new SSE connection
    // ─────────────────────────────────────────────────
    addConnection(res: ServerResponse, orderId: string): SSEConnection | null {
        // Check total connection limit
        const totalActive = this.getActiveConnectionCount();
        if (totalActive >= this.config.maxTotalConnections) {
            log.warn(
                {
                    orderId,
                    activeConnections: totalActive,
                    maxConnections: this.config.maxTotalConnections,
                },
                `⚠️  SSE connection rejected — max connections reached (${totalActive}/${this.config.maxTotalConnections})`
            );
            return null;
        }

        // Check per-order connection limit
        const orderConnections = this.connections.get(orderId);
        if (orderConnections && orderConnections.size >= this.config.maxConnectionsPerOrder) {
            log.warn(
                {
                    orderId,
                    activeForOrder: orderConnections.size,
                    maxPerOrder: this.config.maxConnectionsPerOrder,
                },
                `⚠️  SSE connection rejected — max connections per order reached`
            );
            return null;
        }

        // Create the connection
        const connection = new SSEConnection(
            res,
            orderId,
            this.config.heartbeatIntervalMs,
            this.config.backpressureConfig
        );

        // Add to registry
        if (!this.connections.has(orderId)) {
            this.connections.set(orderId, new Set());
        }

        this.connections.get(orderId)!.add(connection);
        this._totalConnectionsOpened++;

        // Update active connections metric gauge
        sseConnectionsActive.set(this.getActiveConnectionCount());

        // First connection for this order — notify subscriber to listen
        if (this.connections.get(orderId)!.size === 1) {
            this.onFirstConnection?.(orderId);
        }

        // Send initial connection event with retry hint
        connection.send({
            event: 'connected',
            data: {
                connectionId: connection.id,
                orderId,
                message: 'SSE stream established. You will receive realtime order updates.'
            },
            retry: this.config.clientRetryMs,
        });

        // Register cleanup handler
        res.on('close', () => {
            this.removeConnection(connection);
        });

        log.info(
            {
                connectionId: connection.id,
                orderId,
                totalActive: totalActive + 1,
                activeForOrder: this.connections.get(orderId)!.size,
            },
            `📡 SSE connection added (${totalActive + 1} total active)`
        );

        return connection;
    }

    // ─────────────────────────────────────────────────
    // Remove a connection from the registry
    // ─────────────────────────────────────────────────
    private removeConnection(connection: SSEConnection): void {
        const orderConnections = this.connections.get(connection.orderId);
        if (!orderConnections) return;

        orderConnections.delete(connection);
        this._totalConnectionsClosed++;

        // Track backpressure disconnects
        if (connection.state === SSEConnectionState.CLOSED) {
            const stats = connection.backpressureStats;
            if (stats.backpressureEvents > 0) {
                this._backpressureDisconnects++;
                sseBackpressureEventsTotal.inc({ action: 'disconnect' });
            }
            this._eventsDroppedTotal += stats.droppedEvents;
        }

        // Update active connections metric gauge
        sseConnectionsActive.set(this.getActiveConnectionCount());

        // Last connection for this order — notify subscriber to stop listening
        if (orderConnections.size === 0) {
            this.connections.delete(connection.orderId);
            this.onLastDisconnection?.(connection.orderId);

            log.debug(
                { orderId: connection.orderId },
                `No more listeners for order ${connection.orderId} — unsubscribed`
            );
        }
    }

    // ─────────────────────────────────────────────────
    // Broadcast an event to all connections for an order
    // ─────────────────────────────────────────────────
    broadcastToOrder(orderId: string, event: SSEEvent): number {
        const orderConnections = this.connections.get(orderId);
        if (!orderConnections || orderConnections.size === 0) {
            return 0;
        }

        let delivered = 0;
        const toRemove: SSEConnection[] = [];
        for (const connection of orderConnections) {
            if (!connection.isOpen) {
                toRemove.push(connection);
                continue;
            }

            const sent = connection.send(event);
            if (sent) {
                delivered++;
                this._eventsSentTotal++;
                sseEventsSentTotal.inc({ event_type: event.event || 'message' });
            } else {
                this._eventsDroppedTotal++;
                sseBackpressureEventsTotal.inc({ action: 'drop' });
            }
        }

        // Clean up closed connections discovered during broadcast
        for (const dead of toRemove) {
            this.removeConnection(dead);
        }

        log.debug(
            {
                orderId,
                totalListeners: orderConnections.size,
                delivered,
                dropped: orderConnections.size - delivered - toRemove.length,
                removed: toRemove.length,
            },
            `📡 Broadcast to ${delivered}/${orderConnections.size} listeners for order ${orderId}`
        );

        return delivered;
    }

    // ─────────────────────────────────────────────────
    // Get metrics
    // ─────────────────────────────────────────────────
    getMetrics(): SSEMetrics {
        return {
            activeConnections: this.getActiveConnectionCount(),
            activeOrders: this.connections.size,
            totalConnectionsOpened: this._totalConnectionsOpened,
            totalConnectionsClosed: this._totalConnectionsClosed,
            backpressureEventsTotal: this.getBackpressureEventsTotal(),
            backpressureDisconnectsTotal: this._backpressureDisconnects,
            eventsSentTotal: this._eventsSentTotal,
            eventsDroppedTotal: this._eventsDroppedTotal
        };
    }

    private getBackpressureEventsTotal(): number {
        let total = 0;
        for (const conns of this.connections.values()) {
            for (const conn of conns) {
                total += conn.backpressureStats.backpressureEvents;
            }
        }
        return total;
    }

    // ─────────────────────────────────────────────────
    // Check if any connections exist for an order
    // ─────────────────────────────────────────────────
    hasListeners(orderId: string): boolean {
        const conns = this.connections.get(orderId);
        return !!conns && conns.size > 0;
    }

    // ─────────────────────────────────────────────────
    // Get active connection count
    // ─────────────────────────────────────────────────
    getActiveConnectionCount(): number {
        let count = 0;
        for (const conss of this.connections.values()) {
            count += conss.size;
        }
        return count;
    }

    // ─────────────────────────────────────────────────
    // Graceful shutdown — drain all connections
    // ─────────────────────────────────────────────────
    async shutdown(): Promise<void> {
        const activeCount = this.getActiveConnectionCount();
        if (activeCount === 0) {
            log.info('No active SSE connections to drain');
            return;
        }

        log.info(
            { activeConnections: activeCount },
            `📡 Draining ${activeCount} SSE connections...`
        );

        for (const [orderId, conns] of this.connections.entries()) {
            for (const conn of conns) {
                conn.send({
                    event: 'shutdown',
                    data: { message: 'Server is shutting down. Reconnecting...' },
                });
                conn.close('server_shutdown');
            }
            conns.clear();
            this.onLastDisconnection?.(orderId);
        }

        this.connections.clear();
        log.info('✅ All SSE connections drained');
    }
}