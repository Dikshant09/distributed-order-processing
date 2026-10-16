// ============================================================
// Idempotency Store — Preventing Duplicate Event Processing
// ============================================================

import { Pool, PoolClient } from 'pg';

export class IdempotencyStore {
    constructor(private readonly pool: Pool) { }

    // ─────────────────────────────────────────────────
    // Check if an event has already been processed
    // ─────────────────────────────────────────────────
    async hasBeenProcessed(
        eventId: string,
        consumerGroup: string
    ): Promise<boolean> {
        const result = await this.pool.query(
            `SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_group = $2`,
            [eventId, consumerGroup]
        );
        return result.rowCount !== null && result.rowCount > 0;
    }

    // ─────────────────────────────────────────────────
    // Mark an event as processed (within a transaction)
    // ─────────────────────────────────────────────────
    async markAsProcessed(
        eventId: string,
        consumerGroup: string,
        client?: PoolClient
    ): Promise<boolean> {
        const queryable = client ?? this.pool;

        const result = await queryable.query(
            `INSERT INTO processed_events (event_id, consumer_group)
             VALUES ($1, $2)
             ON CONFLICT (event_id, consumer_group) DO NOTHING
             RETURNING event_id`,
            [eventId, consumerGroup]
        );

        return (result.rowCount ?? 0) > 0;
    }
}

export class DuplicateEventError extends Error {
    constructor(public readonly eventId: string) {
        super(`Event ${eventId} has already been processed or is currently being processed concurrently`);
        this.name = 'DuplicateEventError';
    }
}