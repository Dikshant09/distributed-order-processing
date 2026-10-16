// ============================================================
// Order Assignment Service — Distributed Driver Assignment
// ============================================================

import Redis from 'ioredis';
import { createModuleLogger } from '../../shared/logger.js';

const log = createModuleLogger('order-assignment');

export interface AssignmentResult {
    assigned: boolean;
    driverId: string;
    orderId: string;
    expiresAt?: Date;
}

export class OrderAssignmentService {
    private readonly ASSIGNMENT_TTL_MS = 300000; // 5 minutes

    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Assign a driver to an order
    // ─────────────────────────────────────────────────
    async assignDriver(orderId: string, driverId: string): Promise<AssignmentResult> {
        const key = `assignment:order:${orderId}`;

        try {
            const result = await this.redis.set(
                key,
                driverId,
                'PX',
                this.ASSIGNMENT_TTL_MS,
                'NX',
            );

            if (result === 'OK') {
                const expiresAt = new Date(Date.now() + this.ASSIGNMENT_TTL_MS);

                log.info(
                    { orderId, driverId, expiresAt: expiresAt.toISOString() },
                    `🚗 Driver ${driverId} assigned to order ${orderId}`
                );

                return {
                    assigned: true,
                    driverId,
                    orderId,
                    expiresAt
                };
            }

            const currentDriver = await this.redis.get(key);
            log.info(
                { orderId, driverId, currentDriver },
                `🚫 Order ${orderId} already assigned to ${currentDriver}`
            );

            return {
                assigned: false,
                driverId: currentDriver ?? 'unknown',
                orderId,
            };

        } catch (err) {
            log.error({ err, orderId, driverId }, '❌ Failed to assign driver');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Get the current assignment for an order
    // ─────────────────────────────────────────────────
    async getAssignment(orderId: string): Promise<string | null> {
        try {
            return await this.redis.get(`assignment:order:${orderId}`);
        } catch (err) {
            log.error({ err, orderId }, '❌ Failed to get assignment');
            return null;
        }
    }

    // ─────────────────────────────────────────────────
    // Release an assignment
    // ─────────────────────────────────────────────────
    async releaseAssignment(orderId: string, driverId: string): Promise<boolean> {
        try {
            const result = await this.redis.eval(
                `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
                1,
                `assignment:order:${orderId}`,
                driverId
            );

            const released = result === 1;
            if (released) {
                log.info({ orderId, driverId }, `🚗 Assignment released for order ${orderId}`);
            }
            return released;
        } catch (err) {
            log.error({ err, orderId, driverId }, '❌ Failed to release assignment');
            return false;
        }
    }
}