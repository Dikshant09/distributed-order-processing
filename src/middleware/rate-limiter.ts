// ============================================================
// Rate Limiter — Fixed Window Counter with Redis
// ============================================================

import { FastifyRequest, FastifyReply } from 'fastify';
import Redis from 'ioredis';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';
import {
    redisOperationsTotal,
    redisOperationDurationSeconds
} from '../metrics/index.js';

const log = createModuleLogger('rate-limiter');

export function createRateLimiter(redis: Redis) {
    return async function rateLimiter(
        request: FastifyRequest,
        reply: FastifyReply,
    ): Promise<void> {
        const identifier = extractIdentifier(request);

        const windowStart = Math.floor(Date.now() / config.rateLimitWindowMs) * config.rateLimitWindowMs;
        const key = `ratelimit:${identifier}:${windowStart}`;

        try {
            const start = Date.now();
            const results = await redis.multi()
                .incr(key)
                .pexpire(key, config.rateLimitWindowMs)
                .exec();

            redisOperationDurationSeconds.observe(
                { operation: 'rate_limit' },
                (Date.now() - start) / 1000
            );

            if (!results) {
                log.warn('Rate limiter: MULTI/EXEC returned null — falling open');
                return;
            }

            const [incrErr, currentCount] = results[0]!;
            if (incrErr) {
                log.error({ err: incrErr }, 'Rate limiter: INCR failed — falling open');
                return;
            }

            const count = currentCount as number;
            const remaining = Math.max(0, config.rateLimitMaxRequests - count);
            const resetTime = windowStart + config.rateLimitWindowMs;

            reply.header('X-RateLimit-Limit', config.rateLimitMaxRequests);
            reply.header('X-RateLimit-Remaining', remaining);
            reply.header('X-RateLimit-Reset', Math.ceil(resetTime / 1000));

            if (count > config.rateLimitMaxRequests) {
                const retryAfterMs = resetTime - Date.now();
                const retryAfterSec = Math.ceil(retryAfterMs / 1000);

                reply.header('Retry-After', retryAfterSec);

                log.warn(
                    {
                        identifier,
                        count,
                        limit: config.rateLimitMaxRequests,
                        retryAfterSec,
                    },
                    `🚫 Rate limit exceeded for ${identifier}`
                );

                reply.status(429).send({
                    success: false,
                    error: {
                        code: 'RATE_LIMIT_EXCEEDED',
                        message: `Rate limit exceeded. Maximum ${config.rateLimitMaxRequests} requests per ${config.rateLimitWindowMs / 1000} seconds.`,
                        retryAfter: retryAfterSec
                    },
                });
                redisOperationsTotal.inc({ operation: 'rate_limit_reject' });
                return;
            }

            redisOperationsTotal.inc({ operation: 'rate_limit_allow' });
        } catch (err) {
            log.error({ err }, 'Rate limiter error — falling open');
        }
    };

    function extractIdentifier(request: FastifyRequest): string {
        if (request.body && typeof request.body === 'object' && 'customerId' in request.body) {
            return `cust:${(request.body as Record<string, unknown>).customerId}`;
        }
        return `ip:${request.ip}`;
    }
}