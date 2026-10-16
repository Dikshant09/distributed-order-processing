// ============================================================
// Cache Service — Cache-Aside Pattern with Redis
// ============================================================

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger.js';
import {
    redisOperationsTotal,
    redisOperationDurationSeconds
} from '../metrics/index.js';

const log = createModuleLogger('cache');

export class CacheService {
    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Get a value from cache
    // ─────────────────────────────────────────────────
    async get<T>(key: string): Promise<T | null> {
        try {
            const start = Date.now();
            const cached = await this.redis.get(`cache:${key}`);

            redisOperationDurationSeconds.observe(
                { operation: 'cache_get' },
                (Date.now() - start) / 1000
            );

            if (cached) {
                log.debug({ key }, '🎯 Cache HIT');
                redisOperationsTotal.inc({ operation: 'cache_hit' });
                return JSON.parse(cached) as T;
            }
            log.debug({ key }, '❌ Cache MISS');
            redisOperationsTotal.inc({ operation: 'cache_miss' });
            return null;
        } catch (err) {
            log.error({ err, key }, 'Cache GET error — falling through to DB');
            return null;
        }
    }

    // ─────────────────────────────────────────────────
    // Set a value in cache with TTL
    // ─────────────────────────────────────────────────
    async set(key: string, value: unknown, ttlSeconds: number): Promise<void> {
        try {
            const serialized = JSON.stringify(value);
            const start = Date.now();
            await this.redis.setex(`cache:${key}`, ttlSeconds, serialized);

            redisOperationDurationSeconds.observe(
                { operation: 'cache_set' },
                (Date.now() - start) / 1000
            );

            redisOperationsTotal.inc({ operation: 'cache_set' });

            log.debug({ key, ttlSeconds }, '📦 Cache SET');
        } catch (err) {
            log.error({ err, key }, 'Cache SET error — data not cached');
        }
    }

    // ─────────────────────────────────────────────────
    // Invalidate (delete) a cache entry
    // ─────────────────────────────────────────────────
    async invalidate(key: string): Promise<void> {
        try {
            await this.redis.del(`cache:${key}`);
            redisOperationsTotal.inc({ operation: 'cache_invalidate' });
            log.debug({ key }, '🗑️  Cache INVALIDATED');
        } catch (err) {
            log.error({ err, key }, 'Cache INVALIDATE error — stale data may persist until TTL');
        }
    }

    // ─────────────────────────────────────────────────
    // Get-or-Set (cache-aside pattern helper)
    // ─────────────────────────────────────────────────
    async getOrSet<T>(
        key: string,
        fetchFn: () => Promise<T>,
        ttlSeconds: number,
    ): Promise<T> {
        // Try cache first
        const cached = await this.get<T>(key);
        if (cached !== null) {
            return cached;
        }

        // Cache miss — fetch from source
        const fresh = await fetchFn();

        // Store in cache asynchronously
        this.set(key, fresh, ttlSeconds).catch((err => {
            log.error({ err, key }, 'Failed to populate cache after miss');
        }));

        return fresh;
    }
}
