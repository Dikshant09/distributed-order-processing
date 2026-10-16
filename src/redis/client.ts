// ============================================================
// Redis Client — Distributed Coordination Layer
// ============================================================

import Redis from 'ioredis';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('redis-client');

let redisClient: Redis | null = null;

// ─────────────────────────────────────────────────
// Create and connect the Redis client
// ─────────────────────────────────────────────────
export function createRedisClient(): Redis {
    if (redisClient) {
        return redisClient;
    }

    redisClient = new Redis(config.redisUrl, {
        keyPrefix: config.redisKeyPrefix,

        // Retry strategy with capped delay
        retryStrategy(times: number) {
            const delay = Math.min(times * 50, 10000);

            if (times <= 5 || times % 10 === 0) {
                log.warn({ attempt: times, delayMs: delay }, `Redis reconnecting in ${delay}ms`);
            }

            return delay;
        },

        maxRetriesPerRequest: 3,

        // Fail fast when disconnected so operations fall open rather than hanging
        enableOfflineQueue: false,

        // Connection timeout
        connectTimeout: 5000,
    });

    // Connection event handlers
    redisClient.on('connect', () => {
        log.info('✅ Redis connected');
    });

    redisClient.on('ready', () => {
        log.info('✅ Redis ready (accepting commands)');
    });

    redisClient.on('error', (err) => {
        log.error({ err }, '❌ Redis connection error');
    });

    redisClient.on('close', () => {
        log.warn('Redis connection closed');
    });

    redisClient.on('reconnecting', (delay: number) => {
        log.info({ delayMs: delay }, 'Redis reconnecting...');
    });

    return redisClient;
}

// ─────────────────────────────────────────────────
// Get the existing Redis client (or throw)
// ─────────────────────────────────────────────────
export function getRedisClient(): Redis {
    if (!redisClient) {
        throw new Error('Redis client not initialized. Call createRedisClient() first.');
    }
    return redisClient;
}

// ─────────────────────────────────────────────────
// Disconnect Redis
// ─────────────────────────────────────────────────
export async function disconnectRedis(): Promise<void> {
    if (redisClient) {
        await redisClient.quit();
        redisClient = null;
        log.info('✅ Redis disconnected');
    }
}