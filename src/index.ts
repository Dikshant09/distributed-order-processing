// ============================================================
// Entry Point — Application Startup
// ============================================================

import { config } from "./config/index.js";
import { createPool } from "./db/pool.js";
import { buildServer } from "./server.js";
import { setupGracefulShutdown } from "./shared/shutdown.js";
import { logger } from "./shared/logger.js";
import { createKafkaClient } from "./kafka/client.js";
import { EventProducer } from "./kafka/producer.js";
import { OutboxRepository } from "./outbox/outbox.repository.js";
import { OutboxRelay } from "./outbox/outbox-relay.js";
import { createRedisClient } from "./redis/client.js";
import { LeaderElection } from './redis/leader-election.js';
import { SSEManager } from "./sse/sse-manager.js";
import { SSERedisSubscriber } from "./sse/redis-subscriber.js";
import { BackpressurePolicy } from "./sse/types.js";
import { collectDefaultMetrics } from "./metrics/index.js";
import Redis from 'ioredis';

async function main(): Promise<void> {
  logger.info(
    {
      nodeEnv: config.nodeEnv,
      port: config.port,
      kafkaBrokers: config.kafkaBrokers || '(not configured)',
      redisUrl: config.redisUrl || '(not configured)',
      notificationDelayMs: config.notificationDelayMs,
      notificationFailureRate: config.notificationFailureRate,
    },
    "🚀 Starting Order Platform...",
  );

  // Step 0: Initialize Prometheus metrics collection
  if (config.metricsEnabled) {
    collectDefaultMetrics();
    logger.info('📊 Prometheus metrics collection initialized');
  }

  // Step 1: Create database connection pool
  const pool = createPool();

  // Step 2: Verify database connectivity before accepting traffic
  try {
    const result = await pool.query("SELECT NOW() as time");
    logger.info({ dbTime: result.rows[0] }, "✅ Database connection verified");
  } catch (err) {
    logger.fatal({ err }, "❌ Cannot connect to database — exiting");
    process.exit(1);
  }

  // Step 3: Connect Redis (optional)
  let redis: Redis | null = null;
  let leaderElection: LeaderElection | null = null;

  if (config.redisUrl) {
    try {
      redis = createRedisClient();
      let connected = false;
      for (let i = 0; i < 10; i++) {
        try {
          await redis.ping();
          connected = true;
          break;
        } catch (e) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }
      
      if (!connected) {
        throw new Error('Redis connection timeout during startup');
      }
      
      logger.info('✅ Redis connected');
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Redis connection failed — running without Redis (degraded mode)'
      );
      redis = null;
    }
  } else {
    logger.info('ℹ️  Redis not configured — coordination features disabled');
  }

  // Step 4: Connect Kafka producer (for outbox relay)
  let eventProducer: EventProducer | null = null;
  let outboxRelay: OutboxRelay | null = null;
  let relayProducer: ReturnType<ReturnType<typeof createKafkaClient>['producer']> | null = null;

  if (config.kafkaBrokers) {
    try {
      const kafka = createKafkaClient();
      eventProducer = new EventProducer(kafka);
      await eventProducer.connect();

      // Start outbox relay
      relayProducer = kafka.producer({ allowAutoTopicCreation: false });
      await relayProducer.connect();

      const outboxRepo = new OutboxRepository(pool);
      outboxRelay = new OutboxRelay(outboxRepo, relayProducer);

      // Leader election for outbox relay
      if (redis) {
        leaderElection = new LeaderElection(redis);
        leaderElection.start(
          // onBecomeLeader
          () => {
            logger.info('👑 This instance is now the LEADER — starting outbox relay');
            outboxRelay!.start();
          },

          // onLoseLeadership
          () => {
            logger.warn('🏳️  This instance LOST leadership — stopping outbox relay');
            outboxRelay!.stop();
          }
        );
      } else {
        outboxRelay.start();

        logger.info(
          {
            pollIntervalMs: config.outboxPollIntervalMs,
            batchSize: config.outboxBatchSize,
          },
          '📤 Outbox relay started (no leader election — all instances relay)'
        );
      }
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Kafka/Outbox relay failed to start — events will accumulate in outbox'
      );
      eventProducer = null;
      outboxRelay = null;
    }
  } else {
    logger.info('ℹ️  Kafka not configured — outbox relay disabled');
  }

  // Step 6: Create SSE infrastructure
  let sseManager: SSEManager | null = null;
  let sseSubscriber: SSERedisSubscriber | null = null;

  sseManager = new SSEManager({
    maxTotalConnections: config.sseMaxTotalConnections,
    maxConnectionsPerOrder: config.sseMaxConnectionsPerOrder,
    heartbeatIntervalMs: config.sseHeartbeatIntervalMs,
    clientRetryMs: config.sseClientRetryMs,
    backpressureConfig: {
      highWaterMark: config.sseBackpressureHighWaterMark,
      criticalMark: config.sseBackpressureCriticalMark,
      policy: BackpressurePolicy.DROP_NEWEST
    },
  });

  // Redis subscriber for cross-instance fanout
  if (redis && config.redisUrl) {
    try {
      sseSubscriber = new SSERedisSubscriber(
        config.redisUrl,
        sseManager,
        config.redisKeyPrefix
      );

      await sseSubscriber.connect();

      sseManager.setConnectionHandlers(
        (orderId) => { sseSubscriber!.subscribe(orderId); },
        (orderId) => { sseSubscriber!.unsubscribe(orderId); }
      );

      logger.info('✅ SSE Redis subscriber connected (cross-instance fanout enabled)');
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  SSE Redis subscriber failed — SSE works for in-process events only'
      );
      sseSubscriber = null;
    }
  } else {
    logger.info('ℹ️  Redis not available — SSE cross-instance fanout disabled');
  }

  // Step 7: Build and start HTTP server
  const app = await buildServer(pool, redis, sseManager);

  try {
    await app.listen({ port: config.port, host: config.host });
    logger.info(
      {
        port: config.port,
        host: config.host,
      },
      `✅ Server listening on http://${config.host}:${config.port}`
    );
  } catch (err) {
    logger.fatal({ err }, "❌ Failed to start server");
    process.exit(1);
  }

  // Step 8: Register graceful shutdown handlers
  setupGracefulShutdown(app, pool, eventProducer, outboxRelay, leaderElection, sseManager, sseSubscriber);

  // Log startup summary
  logger.info('─'.repeat(60));
  logger.info('📦 Order Platform — Realtime SSE & Backpressure');
  logger.info(`   Environment:  ${config.nodeEnv}`);
  logger.info(`   Port:         ${config.port}`);
  logger.info(`   Kafka:        ${config.kafkaBrokers || 'NOT CONFIGURED'}`);
  logger.info(`   Redis:        ${config.redisUrl || 'NOT CONFIGURED'}`);
  logger.info('');
  logger.info('   Endpoints:');
  logger.info('   POST   /api/v1/orders             — Create order');
  logger.info('   GET    /api/v1/orders              — List orders');
  logger.info('   GET    /api/v1/orders/:id          — Get order (cached)');
  logger.info('   PATCH  /api/v1/orders/:id/status   — Update status (locked)');
  logger.info('   POST   /api/v1/orders/:id/cancel   — Cancel order');
  logger.info('   GET    /api/v1/orders/:id/history  — Status history');
  logger.info('   POST   /api/v1/orders/:id/assign   — Assign driver');
  logger.info('   GET    /api/v1/orders/:id/assignment — Get assignment');
  logger.info('   GET    /api/v1/orders/:id/stream   — SSE realtime stream');
  logger.info('   GET    /health                     — Health check');
  logger.info('');
  if (redis) {
    logger.info('   🔴 Redis:           CONNECTED');
    logger.info(`   🚦 Rate limiting:   ${config.rateLimitMaxRequests} req/${config.rateLimitWindowMs / 1000}s`);
    logger.info(`   📦 Cache TTL:       ${config.cacheOrderTtlSeconds}s`);
    logger.info(`   🔒 Distributed lock: ENABLED`);
    logger.info(`   🗳️  Leader election: ${leaderElection?.isLeader ? 'LEADER' : 'FOLLOWER'}`);
  } else {
    logger.info('   ⚠️  Redis:           NOT CONNECTED (degraded mode)');
    logger.info('   🚦 Rate limiting:   DISABLED');
    logger.info('   📦 Cache:           DISABLED');
    logger.info('   🔒 Distributed lock: DISABLED');
  }
  logger.info('');
  if (sseManager) {
    logger.info(`   📡 SSE:             ENABLED`);
    logger.info(`   📡 Max connections: ${config.sseMaxTotalConnections}`);
    logger.info(`   📡 Heartbeat:       ${config.sseHeartbeatIntervalMs}ms`);
    logger.info(`   📡 Redis fanout:    ${sseSubscriber?.connected ? 'ACTIVE' : 'DISABLED'}`);
  }
  logger.info('');
  if (outboxRelay) {
    const relayStatus = leaderElection
      ? (leaderElection.isLeader ? 'ACTIVE (leader)' : 'STANDBY (follower)')
      : 'ACTIVE (no leader election)';
    logger.info(`   📤 Outbox relay:  ${relayStatus}`);
    logger.info(`   📨 Publishing to: ${config.kafkaTopicOrderEvents}`);
    logger.info(`   ☠️  DLQ topic:     ${config.kafkaTopicDlq}`);
  } else {
    logger.info('   ⚠️  Outbox relay: INACTIVE');
  }
  logger.info('─'.repeat(60));
}

main().catch((err) => {
  logger.fatal({ err }, '❌ Unhandled error during startup');
  process.exit(1);
});
