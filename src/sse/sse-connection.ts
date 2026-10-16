// ============================================================
// SSE Connection — Individual Client Stream
// ============================================================

import { ServerResponse } from 'http';
import { randomUUID } from 'crypto';
import { createModuleLogger } from '../shared/logger.js';
import {
    SSEEvent,
    SSEConnectionState,
    BackpressureConfig
} from './types.js';
import {
    BackpressureTracker,
    DEFAULT_BACKPRESSURE_CONFIG
} from './sse-backpressure.js';

const log = createModuleLogger('sse-connection');

export class SSEConnection {
    readonly id: string;
    readonly orderId: string;
    readonly connectedAt: Date;

    private _state: SSEConnectionState = SSEConnectionState.OPEN;
    private readonly res: ServerResponse;
    private readonly backpressure: BackpressureTracker;
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    private _eventsSent = 0;
    private _lastEventId = 0; // Incrementing ID for Last-Event-ID resume

    constructor(
        res: ServerResponse,
        orderId: string,
        heartbeatIntervalMs: number,
        backpressureConfig: BackpressureConfig = DEFAULT_BACKPRESSURE_CONFIG
    ) {
        this.id = randomUUID();
        this.orderId = orderId;
        this.connectedAt = new Date();
        this.res = res;
        this.backpressure = new BackpressureTracker(this.id, backpressureConfig);

        // Set standard SSE response headers
        this.res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
            'Access-Control-Allow-Origin': '*',
        });

        // Handle client disconnect
        this.res.on('close', () => {
            this.close('client_disconnect');
        });

        // Handle drain event when kernel buffer clears
        this.res.on('drain', () => {
            this.backpressure.onDrain();
            if (this._state === SSEConnectionState.DRAINING) {
                this._state = SSEConnectionState.OPEN;
                log.debug(
                    { connectionId: this.id, orderId: this.orderId },
                    'Connection drained — resuming normal operation'
                );
            }
        });

        // Heartbeat timer to maintain active connection through proxies
        this.heartbeatTimer = setInterval(() => {
            this.sendHeartbeat();
        }, heartbeatIntervalMs);

        this.heartbeatTimer.unref();

        log.info(
            {
                connectionId: this.id,
                orderId: this.orderId,
                heartbeatIntervalMs,
            },
            `📡 SSE connection opened for order ${orderId}`
        );
    }

    // ─────────────────────────────────────────────────
    // Send an SSE event
    // ─────────────────────────────────────────────────
    send(event: SSEEvent): boolean {
        if (this._state === SSEConnectionState.CLOSED) {
            return false;
        }

        const formatted = this.formatSSEMessage(event);
        const bytes = Buffer.byteLength(formatted, 'utf-8');

        // Check backpressure before writing
        const bpAction = this.backpressure.shouldAcceptEvent(bytes);
        if (bpAction.action === 'DROP') {
            return false;
        }

        if (bpAction.action === 'DISCONNECT') {
            this.close('backpressure_disconnect');
            return false;
        }

        const writeAccepted = this.res.write(formatted);
        this.backpressure.recordWrite(bytes, writeAccepted);
        this._eventsSent++;

        if (!writeAccepted) {
            this._state = SSEConnectionState.DRAINING;
            log.debug(
                { connectionId: this.id, orderId: this.orderId, bytes },
                'Write buffer full — entering DRAINING state'
            );
        }

        return true;
    }

    // ─────────────────────────────────────────────────
    // Send heartbeat comment
    // ─────────────────────────────────────────────────
    private sendHeartbeat(): void {
        if (this._state === SSEConnectionState.CLOSED) {
            return;
        }

        const heartbeat = `: heartbeat ${new Date().toISOString()}\n\n`;

        try {
            this.res.write(heartbeat);
        } catch {
            this.close('heartbeat_write_failed');
        }
    }

    // ─────────────────────────────────────────────────
    // Format an SSE message
    // ─────────────────────────────────────────────────
    private formatSSEMessage(event: SSEEvent): string {
        let message = '';

        if (event.event) {
            message += `event: ${event.event}\n`;
        }

        const data = typeof event.data === 'string'
            ? event.data
            : JSON.stringify(event.data);

        const lines = data.split('\n');
        for (const line of lines) {
            message += `data: ${line}\n`;
        }

        if (event.id) {
            message += `id: ${event.id}\n`;
        } else {
            this._lastEventId++;
            message += `id: ${this._lastEventId}\n`;
        }

        if (event.retry !== undefined) {
            message += `retry: ${event.retry}\n`;
        }

        message += '\n';
        return message;
    }

    // ─────────────────────────────────────────────────
    // Close the connection
    // ─────────────────────────────────────────────────
    close(reason: string): void {
        if (this._state === SSEConnectionState.CLOSED) {
            return;
        }

        this._state = SSEConnectionState.CLOSED;

        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        try {
            if (reason !== 'client_disconnect') {
                const closeMessage = `event: close\ndata: ${JSON.stringify({ reason })}\n\n`;
                this.res.write(closeMessage);
            }
            this.res.end();
        } catch {
            // Connection may already be closed
        }

        log.info(
            {
                connectionId: this.id,
                orderId: this.orderId,
                reason,
                eventsSent: this._eventsSent,
                durationMs: Date.now() - this.connectedAt.getTime(),
            },
            `📡 SSE connection closed: ${reason} (sent ${this._eventsSent} events)`
        );
    }

    get state(): SSEConnectionState { return this._state; }
    get eventsSent(): number { return this._eventsSent; }
    get isOpen(): boolean { return this._state !== SSEConnectionState.CLOSED; }
    get backpressureStats() {
        return {
            bufferedBytes: this.backpressure.bufferedBytes,
            isDraining: this.backpressure.isDraining,
            droppedEvents: this.backpressure.droppedEvents,
            backpressureEvents: this.backpressure.backpressureEvents
        };
    }
}