// ============================================================
// Fastify Server — Application Bootstrap
// ============================================================

import Fastify, { FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import { Pool } from "pg";
import Redis from 'ioredis';
import { config } from "./config/index.js";
import { registerOrderRoutes } from "./modules/orders/order.routes.js";
import { registerSSERoutes } from "./sse/sse-routes.js";
import { registerHealthRoutes } from "./shared/health.js";
import { AppError } from "./shared/errors.js";
import { ApiResponse } from "./types/index.js";
import { createRateLimiter } from "./middleware/rate-limiter.js";
import { registerMetricsPlugin } from "./metrics/index.js";
import type { SSEManager } from "./sse/sse-manager.js";

export async function buildServer(
  pool: Pool,
  redis: Redis | null = null,
  sseManager: SSEManager | null = null
): Promise<FastifyInstance> {
  const isDev = config.nodeEnv === "development";

  const app = Fastify({
    logger: isDev
      ? {
        level: config.logLevel,
        base: { service: "order-platform", version: "1.0.0" },
        redact: {
          paths: ["req.headers.authorization", "req.headers.cookie"],
          censor: "[REDACTED]",
        },
        transport: {
          target: "pino-pretty",
          options: {
            colorize: true,
            translateTime: "SYS:HH:MM:ss.l",
            ignore: "pid,hostname",
          },
        },
      }
      : {
        level: config.logLevel,
        base: { service: "order-platform", version: "1.0.0" },
        redact: {
          paths: ["req.headers.authorization", "req.headers.cookie"],
          censor: "[REDACTED]",
        },
      },
    genReqId: () => crypto.randomUUID(),
    connectionTimeout: 10000,
  });

  // ─────────────────────────────────────────────────
  // Plugins
  // ─────────────────────────────────────────────────

  await app.register(cors, {
    origin: true,
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"],
    allowedHeaders: ["Content-Type", "Authorization", "Idempotency-Key"],
  });

  // Prometheus metrics endpoint + HTTP timing hooks
  await registerMetricsPlugin(app);

  // ─────────────────────────────────────────────────
  // Request hooks
  // ─────────────────────────────────────────────────

  app.addHook("onRequest", async (request) => {
    request.log.info(
      {
        method: request.method,
        url: request.url,
        ip: request.ip,
      },
      `→ ${request.method} ${request.url}`,
    );
  });

  // Rate limiting via Redis
  if (redis) {
    const rateLimiter = createRateLimiter(redis);
    app.addHook('onRequest', async (request, reply) => {
      if (request.url === '/health' || request.url === '/metrics') return;
      await rateLimiter(request, reply);
    });
  }

  // Log response with timing
  app.addHook("onResponse", async (request, reply) => {
    request.log.info(
      {
        method: request.method,
        url: request.url,
        statusCode: reply.statusCode,
        responseTimeMs: Math.round(reply.elapsedTime),
      },
      `← ${request.method} ${request.url} ${reply.statusCode} ${Math.round(reply.elapsedTime)}ms`,
    );
  });

  // ─────────────────────────────────────────────────
  // Error handler
  // ─────────────────────────────────────────────────

  app.setErrorHandler(async (error, request, reply) => {
    if (error instanceof AppError) {
      request.log.warn(
        {
          err: error,
          code: error.code,
          statusCode: error.statusCode,
        },
        error.message,
      );

      const response: ApiResponse<never> = {
        success: false,
        error: {
          code: error.code,
          message: error.message,
          ...("details" in error
            ? { details: (error as { details: unknown }).details }
            : {}),
        },
      };

      return reply.status(error.statusCode).send(response);
    }

    request.log.error(
      { err: error },
      "💥 Unhandled error",
    );

    const errorMessage = error instanceof Error ? error.message : String(error);
    const response: ApiResponse<never> = {
      success: false,
      error: {
        code: "INTERNAL_ERROR",
        message:
          config.nodeEnv === "development"
            ? errorMessage
            : "An internal error occurred",
      },
    };

    return reply.status(500).send(response);
  });

  // ─────────────────────────────────────────────────
  // Routes
  // ─────────────────────────────────────────────────

  await registerHealthRoutes(app, pool, redis, sseManager);
  await registerOrderRoutes(app, pool, redis);

  // SSE routes for realtime order streaming
  if (sseManager) {
    await registerSSERoutes(app, pool, sseManager);
  }

  return app;
}
