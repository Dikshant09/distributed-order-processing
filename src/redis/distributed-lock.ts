// ============================================================
// Distributed Lock — Redis-based Mutual Exclusion
// ============================================================

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger';
import {
    redisOperationsTotal,
    redisOperationDurationSeconds
} from '../metrics/index.js';

const log = createModuleLogger('distributed-lock');

// Atomic check-and-delete Lua script to ensure safe lock release
const RELEASE_LOCK_SCRIPT = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
`;

export interface LockResult {
    acquired: boolean;
    token: string | null;
    reason?: 'contention' | 'error';
}

export class DistributedLock {
    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Acquire a non-blocking lock on a resource
    // ─────────────────────────────────────────────────
    async acquireLock(resource: string, ttlMs: number): Promise<LockResult> {
        const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

        try {
            const start = Date.now();
            const result = await this.redis.set(
                `lock:${resource}`,
                token,
                'PX',
                ttlMs,
                'NX'
            );

            redisOperationDurationSeconds.observe(
                { operation: 'lock_acquire' },
                (Date.now() - start) / 1000
            );

            if (result === 'OK') {
                log.debug(
                    { resource, ttlMs, token: token.slice(0, 12) },
                    `🔒 Lock acquired: ${resource}`
                );
                redisOperationsTotal.inc({ operation: 'lock_acquire' });
                return { acquired: true, token };
            }

            log.debug(
                { resource },
                `🔒 Lock NOT acquired (held by another): ${resource}`
            );

            redisOperationsTotal.inc({ operation: 'lock_fail' });
            return { acquired: false, token: null, reason: 'contention' };
        } catch (err) {
            log.error({ err, resource }, '❌ Failed to acquire lock (Redis connection error)');
            return { acquired: false, token: null, reason: 'error' };
        }
    }

    // ─────────────────────────────────────────────────
    // Release a lock using atomic Lua script
    // ─────────────────────────────────────────────────
    async releaseLock(resource: string, token: string): Promise<boolean> {
        try {
            const result = await this.redis.eval(
                RELEASE_LOCK_SCRIPT,
                1,                      // number of KEYS
                `lock:${resource}`,     // KEYS[1]
                token                   // ARGV[1]
            );

            const released = result === 1;
            if (released) {
                redisOperationsTotal.inc({ operation: 'lock_release' });
                log.debug({ resource }, `🔓 Lock released: ${resource}`);
            } else {
                log.warn(
                    { resource },
                    `⚠️ Lock release failed (expired or held by another): ${resource}`
                );
            }
            return released;
        } catch (err) {
            log.error({ err, resource }, '❌ Failed to release lock');
            return false;
        }
    }

    // ─────────────────────────────────────────────────
    // Execute a function while holding a lock
    // ─────────────────────────────────────────────────
    async withLock<T>(
        resource: string,
        ttlMs: number,
        fn: () => Promise<T>
    ): Promise<{ success: boolean; result?: T; error?: string; reason?: 'contention' | 'error' }> {
        const lock = await this.acquireLock(resource, ttlMs);

        if (!lock.acquired || !lock.token) {
            return {
                success: false,
                error: `Could not acquire lock on ${resource}`,
                reason: lock.reason,
            };
        }

        try {
            const result = await fn();
            return { success: true, result };
        } finally {
            await this.releaseLock(resource, lock.token);
        }
    }
}
