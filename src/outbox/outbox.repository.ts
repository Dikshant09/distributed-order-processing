// ============================================================
// Outbox Repository — Transactional Outbox Data Access
// ============================================================

import { Pool, PoolClient } from 'pg';

export interface OutboxEvent {
    id: string;
    aggregateId: string;
    eventType: string;
    topic: string;
    key: string | null;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
    createdAt: Date;
    publishedAt: Date | null;
    status?: string | null;
    lockedAt?: Date | null;
}

export interface InsertOutboxEvent {
    aggregateId: string;
    eventType: string;
    topic: string;
    key: string;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
}

export class OutboxRepository {
    constructor(private readonly pool: Pool) { }

    // ─────────────────────────────────────────────────
    // Insert outbox event within an existing transaction
    // ─────────────────────────────────────────────────
    async insertWithClient(
        client: PoolClient,
        event: InsertOutboxEvent
    ): Promise<void> {
        await client.query(
            `INSERT INTO outbox_events (aggregate_id, event_type, topic, key, payload, headers, status)
             VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')`,
            [
                event.aggregateId,
                event.eventType,
                event.topic,
                event.key,
                JSON.stringify(event.payload),
                JSON.stringify(event.headers),
            ]
        );
    }

    // ─────────────────────────────────────────────────
    // Fetch and claim unpublished events for the relay
    // ─────────────────────────────────────────────────
    // Atomically claim rows for processing with status = 'PROCESSING'
    // using FOR UPDATE SKIP LOCKED to prevent duplicate claims across relay workers.
    async fetchUnpublished(limit: number): Promise<OutboxEvent[]> {
        const result = await this.pool.query<OutboxEventRow>(
            `UPDATE outbox_events
             SET status = 'PROCESSING', locked_at = NOW()
             WHERE id IN (
                 SELECT id FROM outbox_events
                 WHERE published_at IS NULL AND (status IS NULL OR status = 'PENDING' OR (status = 'PROCESSING' AND locked_at < NOW() - INTERVAL '30 seconds'))
                 ORDER BY created_at ASC
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING *`,
            [limit]
        );

        return result.rows.map((row) => this.mapToOutboxEvent(row));
    }

    // ─────────────────────────────────────────────────
    // Mark an event as published
    // ─────────────────────────────────────────────────
    async markPublished(id: string): Promise<void> {
        await this.pool.query(
            `UPDATE outbox_events SET published_at = NOW(), status = 'PUBLISHED' WHERE id = $1`,
            [id]
        );
    }

    // ─────────────────────────────────────────────────
    // Mark multiple events as published (batch)
    // ─────────────────────────────────────────────────
    async markPublishedBatch(ids: string[]): Promise<void> {
        if (ids.length === 0) return;

        await this.pool.query(
            `UPDATE outbox_events SET published_at = NOW(), status = 'PUBLISHED' WHERE id = ANY($1)`,
            [ids]
        );
    }

    // ─────────────────────────────────────────────────
    // Release claimed event on transient failure
    // ─────────────────────────────────────────────────
    async markFailed(id: string): Promise<void> {
        await this.pool.query(
            `UPDATE outbox_events SET status = 'PENDING', locked_at = NULL WHERE id = $1`,
            [id]
        );
    }

    private mapToOutboxEvent(row: OutboxEventRow): OutboxEvent {
        return {
            id: row.id,
            aggregateId: row.aggregate_id,
            eventType: row.event_type,
            topic: row.topic,
            key: row.key,
            payload: row.payload,
            headers: row.headers,
            createdAt: row.created_at,
            publishedAt: row.published_at,
            status: row.status ?? undefined,
            lockedAt: row.locked_at ?? undefined,
        };
    }
}

interface OutboxEventRow {
    id: string;
    aggregate_id: string;
    event_type: string;
    topic: string;
    key: string | null;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
    created_at: Date;
    published_at: Date | null;
    status?: string | null;
    locked_at?: Date | null;
}