// ============================================================
// SSE Backpressure Handler
// ============================================================

import { createModuleLogger } from "../shared/logger.js";
import {
    BackpressureConfig,
    BackpressurePolicy
} from './types.js';

const log = createModuleLogger('sse-backpressure');

// ─────────────────────────────────────────────────
// Default Configuration
// ─────────────────────────────────────────────────
export const DEFAULT_BACKPRESSURE_CONFIG: BackpressureConfig = {
    highWaterMark: 65536,       // 64 KB
    criticalMark: 262144,       // 256 KB
    policy: BackpressurePolicy.DROP_NEWEST
};

// ─────────────────────────────────────────────────
// Backpressure Tracker — Per-Connection State
// ─────────────────────────────────────────────────
export class BackpressureTracker {
    private _bufferedBytes = 0;
    private _isDraining = false;
    private _droppedEvents = 0;
    private _backpressureEvents = 0;
    private _lastWarningTime = 0;

    constructor(
        private readonly connectionId: string,
        private readonly config: BackpressureConfig = DEFAULT_BACKPRESSURE_CONFIG
    ) { }

    // ─────────────────────────────────────────────────
    // Record bytes written
    // ─────────────────────────────────────────────────
    recordWrite(bytes: number, writeAccepted: boolean): void {
        if (writeAccepted) {
            this._bufferedBytes += bytes;
        } else {
            this._bufferedBytes += bytes;
            this._isDraining = true;
            this.checkThresholds();
        }
    }

    // ─────────────────────────────────────────────────
    // Handle drain event
    // ─────────────────────────────────────────────────
    onDrain(): void {
        const previousBytes = this._bufferedBytes;
        this._bufferedBytes = 0;
        this._isDraining = false;

        if (previousBytes > this.config.highWaterMark) {
            log.info(
                {
                    connectionId: this.connectionId,
                    previousBufferedBytes: previousBytes,
                },
                `✅ Backpressure relieved — client caught up (was ${previousBytes} bytes buffered)`
            );
        }
    }

    // ─────────────────────────────────────────────────
    // Should we accept a new event?
    // ─────────────────────────────────────────────────
    shouldAcceptEvent(eventSizeBytes: number): BackpressureAction {
        const projectedSize = this._bufferedBytes + eventSizeBytes;

        // Under high water mark — always accept
        if (projectedSize <= this.config.highWaterMark) {
            return { action: 'ACCEPT' };
        }

        // Between high water mark and critical mark — accept with warning
        if (projectedSize <= this.config.criticalMark) {
            this.emitWarning(projectedSize);
            return { action: 'ACCEPT' };
        }

        // Over critical mark — apply policy
        this._backpressureEvents++;
        return this.applyPolicy(projectedSize);
    }

    // ─────────────────────────────────────────────────
    // Apply backpressure policy
    // ─────────────────────────────────────────────────
    private applyPolicy(projectedSize: number): BackpressureAction {
        switch (this.config.policy) {
            case BackpressurePolicy.DROP_NEWEST:
                this._droppedEvents++;
                log.warn(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        projectedBytes: projectedSize,
                        droppedTotal: this._droppedEvents,
                        policy: 'DROP_NEWEST',
                    },
                    `⚠️  Backpressure: dropping event (buffer would exceed ${this.config.criticalMark} bytes)`
                );
                return { action: 'DROP' };

            case BackpressurePolicy.DROP_OLDEST:
                this._droppedEvents++;
                log.warn(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        policy: 'DROP_OLDEST',
                    },
                    `⚠️  Backpressure: dropping oldest event to make room`
                );
                return { action: 'DROP' };

            case BackpressurePolicy.DISCONNECT:
                log.error(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        projectedBytes: projectedSize,
                        policy: 'DISCONNECT',
                    },
                    `🔌 Backpressure: disconnecting slow client (buffer at ${this._bufferedBytes} bytes)`
                );
                return { action: 'DISCONNECT' };

            default:
                return { action: 'DROP' };
        }
    }

    // ─────────────────────────────────────────────────
    // Throttled warning emission
    // ─────────────────────────────────────────────────
    private checkThresholds(): void {
        this.emitWarning(this._bufferedBytes);
    }

    private emitWarning(bufferSize: number): void {
        const now = Date.now();

        // At most one warning per 10 seconds per connection
        if (now - this._lastWarningTime < 10_000) return;
        this._lastWarningTime = now;

        log.warn(
            {
                connectionId: this.connectionId,
                bufferedBytes: bufferSize,
                highWaterMark: this.config.highWaterMark,
                criticalMark: this.config.criticalMark,
                isDraining: this._isDraining,
            },
            `⚠️  SSE backpressure warning: ${bufferSize} bytes buffered for connection ${this.connectionId}`
        );
    }

    // ─────────────────────────────────────────────────
    // Metrics
    // ─────────────────────────────────────────────────
    get bufferedBytes(): number { return this._bufferedBytes; }
    get isDraining(): boolean { return this._isDraining; }
    get droppedEvents(): number { return this._droppedEvents; }
    get backpressureEvents(): number { return this._backpressureEvents; }
}

// ─────────────────────────────────────────────────
// Backpressure Action — What the caller should do
// ─────────────────────────────────────────────────
export interface BackpressureAction {
    action: 'ACCEPT' | 'DROP' | 'DISCONNECT';
}