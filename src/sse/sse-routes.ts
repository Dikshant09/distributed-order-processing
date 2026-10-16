// ============================================================
// SSE Routes — Realtime Order Streaming Endpoint
// ============================================================

import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { Pool } from 'pg';
import { SSEManager } from './sse-manager.js';
import { OrderService } from '../modules/orders/order.service.js';
import { createModuleLogger } from '../shared/logger';
import { ApiResponse } from '../types/index.js';

const log = createModuleLogger('sse-routes');

export async function registerSSERoutes(
    app: FastifyInstance,
    pool: Pool,
    sseManager: SSEManager
): Promise<void> {
    const orderService = new OrderService(pool);

    // ─────────────────────────────────────────────────
    // GET /api/v1/orders/:id/stream — Realtime Order Updates
    // ─────────────────────────────────────────────────
    app.get('/api/v1/orders/:id/stream',
        async (
            request: FastifyRequest<{ Params: { id: string } }>,
            reply: FastifyReply
        ) => {
            const { id: orderId } = request.params;

            // Step 1: Validate the order exists before opening stream
            try {
                await orderService.getOrder(orderId);
            } catch (err) {
                const response: ApiResponse<never> = {
                    success: false,
                    error: {
                        code: 'NOT_FOUND',
                        message: `Order ${orderId} not found`,
                    },
                };
                return reply.status(404).send(response);
            }

            // Step 2: Hijack response for direct streaming
            await reply.hijack();

            // Disable per-socket idle timeout for long-lived SSE stream
            reply.raw.socket?.setTimeout(0);

            // Step 3: Register the SSE connection
            const connection = sseManager.addConnection(reply.raw, orderId);

            if (!connection) {
                reply.raw.writeHead(503, { 'Content-Type': 'application/json' });
                reply.raw.end(JSON.stringify({
                    success: false,
                    error: {
                        code: 'SERVICE_UNAVAILABLE',
                        message: 'Maximum SSE connections reached. Please retry'
                    },
                }));
                return;
            }

            // Step 4: Send the current order state as the first event to synchronize client
            try {
                const order = await orderService.getOrder(orderId);
                connection.send({
                    event: 'initial_state',
                    data: {
                        orderId: order.id,
                        status: order.status,
                        customerName: order.customerName,
                        restaurantName: order.restaurantName,
                        grandTotal: order.grandTotal,
                        deliveryAddress: order.deliveryAddress,
                        updatedAt: order.updatedAt
                    },
                });
            } catch (err) {
                log.error(
                    { err, orderId, connectionId: connection.id },
                    'Failed to send initial state — connection will receive future updates'
                );
            }

            log.info(
                {
                    orderId,
                    connectionId: connection.id,
                    remoteAddress: request.ip,
                    totalActive: sseManager.getActiveConnectionCount(),
                },
                `📡 SSE stream opened: ${request.method} ${request.url}`
            );
        }
    );
}