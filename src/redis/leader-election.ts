// ============================================================
// Leader Election — Redis Lease-Based Leadership
// ============================================================

import Redis from 'ioredis';
import { config } from '../config';
import { createModuleLogger } from '../shared/logger';
import { hostname } from 'os';

const log = createModuleLogger('leader-election');

// Lua script for renewing the lease (only if we still hold it)
const RENEW_LEASE_SCRIPT = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("PSETEX", KEYS[1], ARGV[2], ARGV[1])
    else
        return nil
    end
`;

export class LeaderElection {
    private readonly instanceId: string;
    private readonly leaderKey = 'leader:outbox-relay';
    private intervalHandle: ReturnType<typeof setInterval> | null = null;
    private _isLeader = false;
    private onBecomeLeader: (() => void) | null = null;
    private onLoseLeadership: (() => void) | null = null;

    constructor(private readonly redis: Redis) {
        this.instanceId = `${hostname()}-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
        log.info({ instanceId: this.instanceId }, 'Leader election initialized');
    }

    get isLeader(): boolean {
        return this._isLeader;
    }

    // ─────────────────────────────────────────────────
    // Start the election loop
    // ─────────────────────────────────────────────────
    start(
        onBecomeLeader: () => void,
        onLoseLeadership: () => void
    ): void {
        this.onBecomeLeader = onBecomeLeader;
        this.onLoseLeadership = onLoseLeadership;

        // Try immediately on start
        this.tryAcquireOrRenew();

        // Then check periodically
        this.intervalHandle = setInterval(
            () => this.tryAcquireOrRenew(),
            config.leaderElectionHeartbeatMs
        );
        this.intervalHandle.unref();

        log.info(
            {
                heartbeatMs: config.leaderElectionHeartbeatMs,
                leaseTtlMs: config.leaderElectionLeaseTtlMs,
                instanceId: this.instanceId,
            },
            '🗳️  Leader election started'
        );
    }

    // ─────────────────────────────────────────────────
    // Stop the election loop
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (this.intervalHandle) {
            clearInterval(this.intervalHandle);
            this.intervalHandle = null;
        }

        // If we're the leader, release the lease gracefully
        if (this._isLeader) {
            try {
                await this.redis.eval(
                    `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
                    1,
                    this.leaderKey,
                    this.instanceId
                );
                log.info('🏳️  Leadership relinquished (graceful shutdown)');
            } catch (err) {
                log.warn({ err }, 'Failed to relinquish leadership — lease will expire');
            }
            this._isLeader = false;
        }

        log.info('🗳️  Leader election stopped');
    }

    private async tryAcquireOrRenew(): Promise<void> {
        try {
            if (this._isLeader) {
                await this.renewLease();
            } else {
                await this.tryAcquire();
            }
        } catch (err) {
            log.error({ err }, '❌ Leader election check failed');
            if (this._isLeader) {
                this._isLeader = false;
                log.warn('⚠️  Lost leadership due to Redis error');
                this.onLoseLeadership?.();
            }
        }
    }

    private async tryAcquire(): Promise<void> {
        const result = await this.redis.set(
            this.leaderKey,
            this.instanceId,
            'PX',
            config.leaderElectionLeaseTtlMs,
            'NX'
        );

        if (result === 'OK') {
            this._isLeader = true;
            log.info(
                { instanceId: this.instanceId },
                '👑 Became LEADER — starting outbox relay'
            );
            this.onBecomeLeader?.();
        } else {
            const currentLeader = await this.redis.get(this.leaderKey);
            log.debug(
                { currentLeader, instanceId: this.instanceId },
                'Following current leader'
            );
        }
    }

    private async renewLease(): Promise<void> {
        const result = await this.redis.eval(
            RENEW_LEASE_SCRIPT,
            1,
            this.leaderKey,
            this.instanceId,
            config.leaderElectionLeaseTtlMs.toString()
        );

        if (result === null) {
            this._isLeader = false;
            log.warn(
                { instanceId: this.instanceId },
                '👑→🏳️  Lost leadership (lease expired or taken)'
            );
            this.onLoseLeadership?.();
        } else {
            log.debug({ instanceId: this.instanceId }, '👑 Leadership renewed');
        }
    }
}