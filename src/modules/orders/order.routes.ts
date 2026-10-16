// ============================================================
// Order Routes — HTTP API Layer
// ============================================================

import { FastifyInstance, FastifyRequest, FastifyReply } from "fastify";
import { Pool } from "pg";
import Redis from 'ioredis';
import { OrderService } from "./order.service.js";
import {
  createOrderSchema,
  updateOrderStatusSchema,
  paginationSchema,
} from "./order.schemas.js";
import { OrderStatusType } from "./order.types.js";
import { createModuleLogger } from "../../shared/logger.js";
import { ApiError, ApiResponse } from "../../types/index.js";
import { Order, OrderStatusHistoryEntry } from "../orders/order.types.js";
import { CacheService } from "../../redis/cache.js";
import { DistributedLock } from "../../redis/distributed-lock.js";
import { OrderAssignmentService } from "./order-assignment.service.js";
import { config } from "../../config/index.js";

const log = createModuleLogger("order-routes");

export async function registerOrderRoutes(
  app: FastifyInstance,
  pool: Pool,
  redis: Redis | null
): Promise<void> {
  const orderService = new OrderService(pool);

  const cache = redis ? new CacheService(redis) : null;
  const lock = redis ? new DistributedLock(redis) : null;
  const assignmentService = redis ? new OrderAssignmentService(redis) : null;

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders — Create Order
  // ─────────────────────────────────────────────────
  app.post(
    "/api/v1/orders",
    async (
      request: FastifyRequest<{
        Body: unknown;
        Headers: { "idempotency-key"?: string };
      }>,
      reply: FastifyReply,
    ) => {
      const startTime = Date.now();

      const parseResult = createOrderSchema.safeParse(request.body);
      if (!parseResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid request body",
            details: parseResult.error.flatten(),
          },
        };
        return reply.status(422).send(response);
      }

      const idempotencyKey = request.headers["idempotency-key"] as
        | string
        | undefined;

      const order = await orderService.createOrder(
        parseResult.data,
        idempotencyKey,
      );

      const latency = Date.now() - startTime;
      log.info(
        { orderId: order.id, latencyMs: latency },
        `POST /api/v1/orders completed ${latency}ms`,
      );

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.status(201).send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id — Get Order by ID
  // ─────────────────────────────────────────────────
  app.get(
    "/api/v1/orders/:id",
    async (
      request: FastifyRequest<{ Params: { id: string } }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;

      let order: Order;
      if (cache) {
        order = await cache.getOrSet<Order>(
          `order:${id}`,
          () => orderService.getOrder(id),
          config.cacheOrderTtlSeconds
        );
      } else {
        order = await orderService.getOrder(id);
      }

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.status(200).send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders — List Orders (Paginated)
  // ─────────────────────────────────────────────────
  app.get(
    "/api/v1/orders",
    async (
      request: FastifyRequest<{
        Querystring: {
          page?: string;
          limit?: string;
          status?: string;
          customerId?: string;
        };
      }>,
      reply: FastifyReply,
    ) => {
      const paginationResult = paginationSchema.safeParse(request.query);
      if (!paginationResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid pagination parameters",
            details: paginationResult.error.flatten(),
          },
        };

        return reply.status(422).send(response);
      }

      const { page, limit } = paginationResult.data;
      const filters: { status?: OrderStatusType; customerId?: string } = {};

      if (request.query.status) {
        filters["status"] = request.query.status as OrderStatusType;
      }

      if (request.query.customerId) {
        filters["customerId"] = request.query.customerId;
      }

      const result = await orderService.listOrders(page, limit, filters);

      const response: ApiResponse<Order[]> = {
        success: true,
        data: result.orders,
        meta: {
          page: result.page,
          limit: result.limit,
          total: result.total,
          totalPages: result.totalPages,
        },
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // PATCH /api/v1/orders/:id/status — Update Status
  // ─────────────────────────────────────────────────
  app.patch(
    "/api/v1/orders/:id/status",
    async (
      request: FastifyRequest<{ Params: { id: string }; Body: unknown }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;

      const parseResult = updateOrderStatusSchema.safeParse(request.body);
      if (!parseResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid request body",
            details: parseResult.error.flatten(),
          },
        };
        return reply.status(422).send(response);
      }

      if (lock) {
        const lockResult = await lock.withLock(
          `order:${id}`,
          5000,
          async () => {
            const order = await orderService.updateOrderStatus(id, parseResult.data);
            await cache?.invalidate(`order:${id}`);
            return order;
          }
        );

        if (!lockResult.success) {
          if (lockResult.reason === 'error') {
            log.warn(
              { orderId: id },
              '⚠️ Redis unavailable — falling through to no-lock status update'
            );
          } else {
            const response: ApiResponse<never> = {
              success: false,
              error: {
                code: 'CONFLICT',
                message: 'Order is being updated by another request. Please retry'
              }
            };
            return reply.status(409).send(response);
          }
        } else {
          const response: ApiResponse<Order> = {
            success: true,
            data: lockResult.result!,
          };

          return reply.send(response);
        }
      }

      // Fallback without Redis: no lock, just update
      const order = await orderService.updateOrderStatus(id, parseResult.data);

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders/:id/cancel — Cancel Order
  // ─────────────────────────────────────────────────
  app.post(
    "/api/v1/orders/:id/cancel",
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: { reason?: string } | undefined;
      }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;
      const reason =
        request.body &&
          typeof request.body === "object" &&
          "reason" in request.body
          ? (request.body as { reason?: string }).reason
          : undefined;

      const order = await orderService.cancelOrder(id, reason);
      await cache?.invalidate(`order:${id}`);

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id/history — Get Status History
  // ─────────────────────────────────────────────────
  app.get(
    "/api/v1/orders/:id/history",
    async (
      request: FastifyRequest<{
        Params: { id: string };
      }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;
      const history = await orderService.getOrderStatusHistory(id);

      const response: ApiResponse<OrderStatusHistoryEntry[]> = {
        success: true,
        data: history,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders/:id/assign — Assign Driver
  // ─────────────────────────────────────────────────
  app.post(
    '/api/v1/orders/:id/assign',
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: unknown;
      }>,
      reply: FastifyReply
    ) => {
      if (!assignmentService) {
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Order assignment requires Redis. Redis is not configured.',
          }
        };
        return reply.status(503).send(response);
      }

      const { id: orderId } = request.params;

      const body = request.body as Record<string, unknown> | null;
      const driverId = body?.driverId as string | undefined;

      if (!driverId || typeof driverId !== 'string' || driverId.trim() === '') {
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'driverId is required and must be a non-empty string',
          }
        };
        return reply.status(422).send(response);
      }

      try {
        const result = await assignmentService.assignDriver(orderId, driverId.trim());

        if (result.assigned) {
          const response: ApiResponse<typeof result> = {
            success: true,
            data: result
          };

          return reply.status(201).send(response);
        }

        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'ALREADY_ASSIGNED',
            message: `Order ${orderId} is already assigned to driver ${result.driverId}.`
          }
        };
        return reply.status(409).send(response);
      } catch (err) {
        request.log.error({ err, orderId, driverId }, "Redis error during driver assignment");
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Order assignment requires Redis, which is currently unavailable.'
          }
        };
        return reply.status(503).send(response);
      }
    }
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id/assignment — Get Assignment
  // ─────────────────────────────────────────────────
  app.get(
    '/api/v1/orders/:id/assignment',
    async (
      request: FastifyRequest<{ Params: { id: string } }>,
      reply: FastifyReply
    ) => {
      if (!assignmentService) {
        return reply.status(503).send({
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Redis is not configured.'
          }
        });
      }

      const { id: orderId } = request.params;
      const driverId = await assignmentService.getAssignment(orderId);

      if (!driverId) {
        return reply.status(404).send({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `No driver assigned to order ${orderId}`
          }
        });
      }

      return reply.send({
        success: true,
        data: { orderId, driverId }
      });
    }
  );
}
