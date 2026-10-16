// ============================================================
// Graceful Shutdown
// ============================================================

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import { createModuleLogger } from "./logger.js";
import { config } from "../config/index.js";
import { EventProducer } from "../kafka/producer.js";
import { OutboxRelay } from "../outbox/outbox-relay.js";
import { LeaderElection } from "../redis/leader-election.js";
import { disconnectRedis } from "../redis/client.js";
import { SSEManager } from "../sse/sse-manager.js";
import { SSERedisSubscriber } from "../sse/redis-subscriber.js";

const log = createModuleLogger("shutdown");

export function setupGracefulShutdown(
  app: FastifyInstance,
  pool: Pool,
  eventProducer: EventProducer | null = null,
  outboxRelay: OutboxRelay | null = null,
  leaderElection: LeaderElection | null = null,
  sseManager: SSEManager | null = null,
  sseSubscriber: SSERedisSubscriber | null = null
): void {
  let isShuttingDown = false;

  async function shutdown(signal: string): Promise<void> {
    if (isShuttingDown) {
      log.warn({ signal }, "Shutdown already in process, ignoring signal");
      return;
    }

    isShuttingDown = true;
    log.info({ signal }, "🔄 Graceful shutdown initiated");

    // Force exit after timeout as safety net
    const forceExitTimer = setTimeout(() => {
      log.error("❌ Graceful shutdown timed out, forcing exit");
      process.exit(1);
    }, config.shutdownTimeoutMs);

    forceExitTimer.unref();

    try {
      // Step 1: Drain SSE connections
      if (sseManager) {
        log.info('Draining SSE connections...');
        await sseManager.shutdown();
        log.info('✅ SSE connections drained');
      }

      // Step 2: Stop accepting new HTTP connections
      log.info("Closing HTTP Server (no new connections)...");
      await app.close();
      log.info("✅ HTTP server closed");

      // Step 3: Stop leader election
      if (leaderElection) {
        log.info('Stopping leader election...');
        await leaderElection.stop();
        log.info('✅ Leader election stopped');
      }

      // Step 4: Stop outbox relay
      if (outboxRelay) {
        log.info('Stopping outbox relay...');
        await outboxRelay.stop();
        log.info('✅ Outbox relay stopped');
      }

      // Step 5: Disconnect Kafka producer
      if (eventProducer) {
        log.info('Disconnecting Kafka producer...');
        await eventProducer.disconnect();
        log.info('✅ Kafka producer disconnected');
      }

      // Step 6: Disconnect SSE Redis subscriber
      if (sseSubscriber) {
        log.info('Disconnecting SSE Redis subscriber...');
        await sseSubscriber.disconnect();
        log.info('✅ SSE Redis subscriber disconnected');
      }

      // Step 7: Disconnect Redis (main)
      log.info('Disconnecting Redis...');
      await disconnectRedis();
      log.info('✅ Redis disconnected');

      // Step 8: Close database pool
      log.info("Closing database pool...");
      await pool.end();
      log.info("✅ Database pool closed");

      log.info("✅ Graceful shutdown complete");
      process.exit(0);
    } catch (err) {
      log.error({ err }, "❌ Error during graceful shutdown");
      process.exit(1);
    }
  }

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  process.on("unhandledRejection", (reason) => {
    log.fatal({ err: reason }, "Unhandled promise rejection — crashing");
    process.exit(1);
  });

  process.on("uncaughtException", (err) => {
    log.fatal({ err }, "Uncaught exception — crashing");
    process.exit(1);
  });
}
