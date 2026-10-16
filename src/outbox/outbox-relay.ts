// ============================================================
// Outbox Relay — Polling Publisher Pattern
// ============================================================

import { Producer } from "kafkajs";
import { OutboxRepository, OutboxEvent } from "./outbox.repository";
import { config } from "../config";
import { createModuleLogger } from "../shared/logger";
import {
    outboxEventsRelayedTotal,
    outboxRelayPollDurationSeconds,
    outboxEventsPending,
} from '../metrics/index.js';

const log = createModuleLogger('outbox-relay');

export class OutboxRelay {
    private intervalHandle: ReturnType<typeof setInterval> | null = null;
    private isRunning = false;
    private isProcessing = false;

    constructor(
        private readonly outboxRepo: OutboxRepository,
        private readonly producer: Producer
    ) { }

    // ─────────────────────────────────────────────────
    // Start the polling loop
    // ─────────────────────────────────────────────────
    start(): void {
        if (this.isRunning) {
            log.warn('Outbox relay is already running');
            return;
        }

        this.isRunning = true;

        this.intervalHandle = setInterval(
            () => this.poll(),
            config.outboxPollIntervalMs
        );

        // Unref so the timer doesn't prevent Node.js from exiting during shutdown
        this.intervalHandle.unref();

        log.info(
            {
                pollIntervalMs: config.outboxPollIntervalMs,
                batchSize: config.outboxBatchSize
            },
            '✅ Outbox relay started'
        );

        // Run an initial poll immediately
        this.poll();
    }

    // ─────────────────────────────────────────────────
    // Stop the polling loop
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (!this.isRunning) return;

        this.isRunning = false;

        if (this.intervalHandle) {
            clearInterval(this.intervalHandle);
            this.intervalHandle = null;
        }

        // Wait for any in-progress poll to complete
        let waitAttempts = 0;
        while (this.isProcessing && waitAttempts < 10) {
            await new Promise((resolve) => setTimeout(resolve, 200));
            waitAttempts++;
        }

        log.info('✅ Outbox relay stopped');
    }

    // ─────────────────────────────────────────────────
    // Single poll cycle
    // ─────────────────────────────────────────────────
    async poll(): Promise<void> {
        // Guard against overlapping polls
        if (this.isProcessing || !this.isRunning) return;
        this.isProcessing = true;
        const pollStartTime = Date.now();

        try {
            const events = await this.outboxRepo.fetchUnpublished(config.outboxBatchSize);

            if (events.length === 0) {
                return;
            }

            log.debug({ count: events.length }, `📦 Relaying ${events.length} outbox events`);

            outboxEventsPending.set(events.length);
            let publishedCount = 0;
            for (const event of events) {
                try {
                    await this.relayEvent(event);
                    publishedCount++;

                    outboxEventsRelayedTotal.inc({ status: 'success' });
                } catch (err) {
                    log.error(
                        {
                            err,
                            outboxId: event.id,
                            eventType: event.eventType,
                            aggregateId: event.aggregateId
                        },
                        `❌ Failed to relay outbox event — will retry next poll`
                    );

                    outboxEventsRelayedTotal.inc({ status: 'failure' });

                    // Release status claim so event can be retried immediately on the next poll
                    await this.outboxRepo.markFailed(event.id);
                }
            }

            if (publishedCount > 0) {
                log.info(
                    { published: publishedCount, total: events.length },
                    `📤 Relayed ${publishedCount}/${events.length} outbox events to Kafka`
                );
            }
        } catch (err) {
            log.error({ err }, "❌ Outbox relay poll failed");
        } finally {
            this.isProcessing = false;
            outboxRelayPollDurationSeconds.observe((Date.now() - pollStartTime) / 1000);
        }
    }

    // ─────────────────────────────────────────────────
    // Relay a single event to Kafka
    // ─────────────────────────────────────────────────
    private async relayEvent(event: OutboxEvent): Promise<void> {
        // Convert stored headers object to Kafka headers format
        const kafkaHeaders: Record<string, string> = {};
        if (event.headers && typeof event.headers === 'object') {
            for (const [key, value] of Object.entries(event.headers)) {
                kafkaHeaders[key] = String(value);
            }
        }

        // Produce to Kafka
        await this.producer.send({
            topic: event.topic,
            messages: [
                {
                    key: event.key ?? undefined,
                    value: JSON.stringify(event.payload),
                    headers: kafkaHeaders
                },
            ],
            acks: -1, // Wait for all in-sync replicas
        });

        // Mark as published
        this.outboxRepo.markPublished(event.id);
    }
}