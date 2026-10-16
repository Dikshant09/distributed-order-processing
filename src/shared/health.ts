// ============================================================
// Health Check
// ============================================================

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import Redis from 'ioredis';
import { createModuleLogger } from "./logger.js";
import { SSEManager } from "../sse/sse-manager.js";
import { SSEMetrics } from "../sse/types.js";

const log = createModuleLogger("health");

interface HealthCheckResult {
  status: "healthy" | "degraded" | "unhealthy";
  uptime: number;
  timestamp: string;
  version: string;
  checks: {
    database: {
      status: "up" | "down";
      latencyMs: number;
      poolSize: {
        total: number;
        idle: number;
        waiting: number;
      };
    };
    redis: {
      status: 'up' | 'down' | 'not_configured';
      latencyMs: number;
    };
    sse: {
      status: 'active' | 'disabled';
      metrics: SSEMetrics | null;
    };
  };
}

export async function registerHealthRoutes(
  app: FastifyInstance,
  pool: Pool,
  redis: Redis | null = null,
  sseManager: SSEManager | null = null
): Promise<void> {
  app.get("/health", async (_request, reply) => {
    let dbStatus: "up" | "down" = "down";
    let dbLatency = 0;
    let redisStatus: 'up' | 'down' | 'not_configured' = 'not_configured';
    let redisLatency = 0;

    try {
      const dbStart = Date.now();
      await pool.query("SELECT 1");
      dbLatency = Date.now() - dbStart;
      dbStatus = "up";
    } catch (err) {
      log.error({ err }, "Health check: database connection failed");
    }

    if (redis) {
      try {
        const redisStart = Date.now();
        await redis.ping();
        redisLatency = Date.now() - redisStart;
        redisStatus = 'up';
      } catch (err) {
        log.error({ err }, "Health check: redis connection failed");
        redisStatus = 'down';
      }
    }

    let overallStatus: 'healthy' | 'degraded' | 'unhealthy';
    if (dbStatus === 'down') {
      overallStatus = 'unhealthy';
    } else if (redisStatus === 'down') {
      overallStatus = 'degraded';
    } else {
      overallStatus = 'healthy';
    }

    const result: HealthCheckResult = {
      status: overallStatus,
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      version: "1.0.0",
      checks: {
        database: {
          status: dbStatus,
          latencyMs: dbLatency,
          poolSize: {
            total: pool.totalCount,
            idle: pool.idleCount,
            waiting: pool.waitingCount,
          },
        },
        redis: {
          status: redisStatus,
          latencyMs: redisLatency
        },
        sse: {
          status: sseManager ? 'active' : 'disabled',
          metrics: sseManager ? sseManager.getMetrics() : null
        }
      },
    };

    const statusCode = result.status === "unhealthy" ? 503 : 200;
    return reply.status(statusCode).send(result);
  });
}
