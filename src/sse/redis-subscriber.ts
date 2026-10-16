// ============================================================
// Redis Pub/Sub Subscriber — SSE Fanout Bridge
// ============================================================

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger';
import { SSEManager } from './sse-manager';
import { RedisPubSubMessage } from './types.js';

const log = createModuleLogger('sse-redis-subscriber');

export class SSERedisSubscriber {
    private subscriber: Redis;
    private readonly activeChannels = new Set<string>();
    private isConnected = false;

    constructor(
        redisUrl: string,
        private readonly sseManager: SSEManager,
        private readonly keyPrefix: string = 'op:'
    ) {
        // Create a dedicated connection for subscribing
        this.subscriber = new Redis(redisUrl, {
            lazyConnect: true,

            retryStrategy(times: number) {
                const delay = Math.min(times * 50, 10000);
                if (times <= 5 || times % 10 === 0) {
                    log.warn({ attempt: times, delayMs: delay }, 'Redis subscriber reconnecting...');
                }
                return delay;
            },

            maxRetriesPerRequest: null,
            connectTimeout: 5000,
        });

        // ─────────────────────────────────────────────────
        // Handle incoming messages
        // ─────────────────────────────────────────────────
        this.subscriber.on('message', (channel: string, message: string) => {
            this.handleMessage(channel, message);
        });

        // ─────────────────────────────────────────────────
        // Handle reconnection
        // ─────────────────────────────────────────────────
        this.subscriber.on('ready', () => {
            log.info('✅ Redis subscriber connected');
            this.isConnected = true;
            this.resubscribeAll();
        });

        this.subscriber.on('error', (err) => {
            log.error({ err }, '❌ Redis subscriber error');
        });

        this.subscriber.on('close', () => {
            log.warn('Redis subscriber connection closed');
            this.isConnected = false;
        });
    }

    // ─────────────────────────────────────────────────
    // Connect to Redis
    // ─────────────────────────────────────────────────
    async connect(): Promise<void> {
        try {
            await this.subscriber.connect();
            log.info('✅ Redis subscriber started');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect Redis subscriber');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Subscribe to an order channel
    // ─────────────────────────────────────────────────
    async subscribe(orderId: string): Promise<void> {
        const channel = this.getChannelName(orderId);

        if (this.activeChannels.has(channel)) {
            return;
        }

        this.activeChannels.add(channel);

        if (this.isConnected) {
            try {
                await this.subscriber.subscribe(channel);
                log.debug(
                    { channel, orderId, totalChannels: this.activeChannels.size },
                    `📡 Subscribed to Redis channel: ${channel}`
                );
            } catch (err) {
                log.error({ err, channel }, 'Failed to subscribe to Redis channel');
                this.activeChannels.delete(channel);
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Unsubscribe from an order channel
    // ─────────────────────────────────────────────────
    async unsubscribe(orderId: string): Promise<void> {
        const channel = this.getChannelName(orderId);

        if (!this.activeChannels.has(channel)) {
            return;
        }

        this.activeChannels.delete(channel);

        if (this.isConnected) {
            try {
                await this.subscriber.unsubscribe(channel);
                log.debug(
                    { channel, orderId, totalChannels: this.activeChannels.size },
                    `📡 Unsubscribed from Redis channel: ${channel}`
                );
            } catch (err) {
                log.error({ err, channel }, 'Failed to unsubscribe from Redis channel');
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Handle incoming Redis Pub/Sub message
    // ─────────────────────────────────────────────────
    private handleMessage(channel: string, rawMessage: string): void {
        const orderId = this.extractOrderId(channel);
        if (!orderId) {
            log.warn({ channel }, 'Received message on unknown channel format');
            return;
        }

        let message: RedisPubSubMessage;
        try {
            message = JSON.parse(rawMessage) as RedisPubSubMessage;
        } catch (err) {
            log.error(
                { err, channel, rawMessage: rawMessage.slice(0, 200) },
                '❌ Failed to parse Redis Pub/Sub message'
            );
            return;
        }

        const delivered = this.sseManager.broadcastToOrder(orderId, {
            event: message.eventType,
            data: message.data
        });

        log.debug(
            {
                channel,
                orderId,
                eventType: message.eventType,
                delivered,
            },
            `📡 Redis → SSE: ${message.eventType} for order ${orderId} (${delivered} clients)`
        );
    }

    // ─────────────────────────────────────────────────
    // Re-subscribe to all active channels after reconnect
    // ─────────────────────────────────────────────────
    private async resubscribeAll(): Promise<void> {
        if (this.activeChannels.size === 0) return;

        log.info(
            { channelCount: this.activeChannels.size },
            `🔄 Re-subscribing to ${this.activeChannels.size} channels after reconnect`
        );

        const channels = Array.from(this.activeChannels);
        try {
            await this.subscriber.subscribe(...channels);
            log.info(
                { channelCount: channels.length },
                `✅ Re-subscribed to ${channels.length} channels`
            );
        } catch (err) {
            log.error(
                { err, channelCount: channels.length },
                '❌ Failed to re-subscribe after reconnect'
            );
        }
    }

    private getChannelName(orderId: string): string {
        return `${this.keyPrefix}sse:order:${orderId}`;
    }

    private extractOrderId(channel: string): string | null {
        const prefix = `${this.keyPrefix}sse:order:`;
        if (channel.startsWith(prefix)) {
            return channel.slice(prefix.length);
        }
        return null;
    }

    // ─────────────────────────────────────────────────
    // Disconnect
    // ─────────────────────────────────────────────────
    async disconnect(): Promise<void> {
        if (!this.isConnected) return;

        try {
            if (this.activeChannels.size > 0) {
                const channels = Array.from(this.activeChannels);
                await this.subscriber.unsubscribe(...channels);
                this.activeChannels.clear();
            }

            await this.subscriber.quit();
            this.isConnected = false;
            log.info('✅ Redis subscriber disconnected');
        } catch (err) {
            log.error({ err }, 'Error disconnecting Redis subscriber');
            this.subscriber.disconnect();
            this.isConnected = false;
        }
    }

    get connected(): boolean { return this.isConnected; }
    get channelCount(): number { return this.activeChannels.size; }
}

// ─────────────────────────────────────────────────
// Static helper: Publish to Redis Pub/Sub
// ─────────────────────────────────────────────────
export async function publishOrderUpdate(
    redis: Redis,
    keyPrefix: string,
    message: RedisPubSubMessage
): Promise<void> {
    const channel = `${keyPrefix}sse:order:${message.orderId}`;
    const serialized = JSON.stringify(message);

    try {
        const subscriberCount = await redis.publish(channel, serialized);

        log.debug(
            {
                channel,
                orderId: message.orderId,
                eventType: message.eventType,
                subscriberCount,
            },
            `📡 Published to Redis: ${message.eventType} for order ${message.orderId} (${subscriberCount} subscribers)`
        );
    } catch (err) {
        log.error(
            { err, channel, orderId: message.orderId },
            '❌ Failed to publish to Redis Pub/Sub (SSE update will be missed)'
        );
    }
}