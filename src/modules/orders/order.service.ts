// ============================================================
// Order Service — Business Logic
// ============================================================
import { Pool } from "pg";
import { OrderRepository } from "./order.repository";
import {
  Order,
  OrderStatusHistoryEntry,
  CreateOrderInput,
  UpdateOrderStatusInput,
  VALID_STATUS_TRANSITIONS,
  OrderStatusType,
} from "./order.types.js";
import { OutboxRepository, InsertOutboxEvent } from '../../outbox/outbox.repository.js';
import {
  buildOrderCreatedEvent,
  buildOrderStatusChangedEvent,
} from '../../kafka/events.js';
import { config } from "../../config";
import { NotFoundError, ConflictError } from "../../shared/errors.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-service");

export class OrderService {
  private readonly orderRepo: OrderRepository;
  private readonly outboxRepo: OutboxRepository;

  constructor(pool: Pool) {
    this.orderRepo = new OrderRepository(pool);
    this.outboxRepo = new OutboxRepository(pool);
  }

  // ─────────────────────────────────────────────────
  // CREATE ORDER
  // ─────────────────────────────────────────────────
  async createOrder(
    input: CreateOrderInput,
    idempotencyKey?: string,
  ): Promise<Order> {
    const startTime = Date.now();

    // Step 1: Idempotency check
    if (idempotencyKey) {
      const existingOrder =
        await this.orderRepo.findByIdempotencyKey(idempotencyKey);

      if (existingOrder) {
        log.info(
          {
            orderId: existingOrder.id,
            idempotencyKey,
          },
          "Returning existing order (idempotent request)",
        );
        return existingOrder;
      }
    }

    // Step 2: Calculate totals
    const totalAmount = input.items.reduce(
      (sum, item) => sum + item.quantity * item.unitPrice,
      0,
    );

    // Round to avoid floating point issues
    const roundedTotalAmount = Math.round(totalAmount * 100) / 100;
    const deliveryFee = this.calculateDeliveryFee(roundedTotalAmount);
    const taxAmount = Math.round(roundedTotalAmount * 0.08 * 100) / 100; // 8% tax
    const grandTotal =
      Math.round((roundedTotalAmount + deliveryFee + taxAmount) * 100) / 100;

    // Step 3: Create order in database atomically with transactional outbox event
    let order: Order;
    try {
      order = await this.orderRepo.create(
        input,
        idempotencyKey,
        {
          totalAmount: roundedTotalAmount,
          deliveryFee,
          taxAmount,
          grandTotal,
        },
        async (client) => {
          const result = await client.query<{ id: string }>(
            `SELECT id FROM orders WHERE idempotency_key = $1
             UNION ALL
             SELECT id FROM orders WHERE customer_id = $2
             ORDER BY 1 DESC LIMIT 1`,
            [idempotencyKey ?? '', input.customerId]
          );

          const orderId = result.rows[0]?.id ?? 'unknown';

          const event = buildOrderCreatedEvent({
            id: orderId,
            customerId: input.customerId,
            customerName: input.customerName,
            customerEmail: input.customerEmail,
            customerPhone: input.customerPhone,
            restaurantId: input.restaurantId,
            restaurantName: input.restaurantName,
            status: 'PENDING',
            totalAmount: roundedTotalAmount,
            deliveryFee,
            taxAmount,
            grandTotal,
            deliveryAddress: input.deliveryAddress,
            items: input.items.map((item) => ({
              itemName: item.itemName,
              quantity: item.quantity,
              unitPrice: item.unitPrice,
              totalPrice: Math.round(item.quantity * item.unitPrice * 100) / 100
            })),
          });

          const outboxEvent: InsertOutboxEvent = {
            aggregateId: orderId,
            eventType: event.eventType,
            topic: config.kafkaTopicOrderEvents,
            key: orderId,
            payload: event as unknown as Record<string, unknown>,
            headers: {
              'event-type': event.eventType,
              'event-id': event.eventId,
              'source': event.source,
            }
          };

          await this.outboxRepo.insertWithClient(client, outboxEvent);
        }
      );
    } catch (err: unknown) {
      // Handle unique constraint violation on idempotency_key (code 23505)
      if (
        err instanceof Error &&
        "code" in err &&
        (err as Record<string, unknown>).code === "23505" &&
        idempotencyKey
      ) {
        const existingOrder =
          await this.orderRepo.findByIdempotencyKey(idempotencyKey);

        if (existingOrder) {
          log.info(
            {
              orderId: existingOrder.id,
              idempotencyKey,
            },
            "Returning existing order (concurrent idempotent request)",
          );
          return existingOrder;
        }
      }

      throw err;
    }

    log.info(
      {
        orderId: order.id,
        customerId: order.customerId,
        itemCount: order.items.length,
        grandTotal: order.grandTotal,
      },
      '📦 Order created (event written to outbox)'
    );

    const totalLatency = Date.now() - startTime;
    log.info(
      {
        orderId: order.id,
        totalLatencyMs: totalLatency,
      },
      `Order creation completed in ${totalLatency}ms`,
    );

    return order;
  }

  // ─────────────────────────────────────────────────
  // GET ORDER
  // ─────────────────────────────────────────────────
  async getOrder(id: string): Promise<Order> {
    const order = await this.orderRepo.findById(id);
    if (!order) {
      throw new NotFoundError("Order", id);
    }
    return order;
  }

  // ─────────────────────────────────────────────────
  // LIST ORDERS
  // ─────────────────────────────────────────────────
  async listOrders(
    page: number,
    limit: number,
    filters?: { status?: OrderStatusType; customerId?: string },
  ): Promise<{
    orders: Order[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { orders, total } = await this.orderRepo.findAll(
      page,
      limit,
      filters,
    );
    return {
      orders,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  // ─────────────────────────────────────────────────
  // UPDATE ORDER STATUS
  // ─────────────────────────────────────────────────
  async updateOrderStatus(
    id: string,
    input: UpdateOrderStatusInput,
  ): Promise<Order> {
    // Step 1: Get current order
    const currentOrder = await this.orderRepo.findById(id);
    if (!currentOrder) {
      throw new NotFoundError("order", id);
    }

    // Step 2: Validate state transition against state machine
    const allowedTransitions = VALID_STATUS_TRANSITIONS[currentOrder.status];
    if (!allowedTransitions?.includes(input.status)) {
      throw new ConflictError(
        `Cannot transition from ${currentOrder.status} to ${input.status}. ` +
        `Allowed transitions: ${allowedTransitions?.join(", ") || "none (terminal state)"}`,
        "INVALID_STATUS_TRANSITION",
      );
    }

    // Step 3: Update status with optimistic concurrency + transactional outbox
    const updatedOrder = await this.orderRepo.updateStatus(
      id,
      currentOrder.status,
      input.status,
      input.changedBy ?? "system",
      input.reason,
      async (client) => {
        const event = buildOrderStatusChangedEvent({
          orderId: id,
          previousStatus: currentOrder.status,
          newStatus: input.status,
          changedBy: input.changedBy ?? 'system',
          reason: input.reason,
          customerEmail: currentOrder.customerEmail,
          customerName: currentOrder.customerName,
          restaurantName: currentOrder.restaurantName,
        });

        const outboxEvent: InsertOutboxEvent = {
          aggregateId: id,
          eventType: event.eventType,
          topic: config.kafkaTopicOrderEvents,
          key: id,
          payload: event as unknown as Record<string, unknown>,
          headers: {
            'event-type': event.eventType,
            'event-id': event.eventId,
            'source': event.source
          },
        };

        await this.outboxRepo.insertWithClient(client, outboxEvent);
      }
    );

    if (!updatedOrder) {
      throw new ConflictError(
        "Order status was modified by another request. Please retry",
        "CONCURRENT_MODIFICATION",
      );
    }

    log.info(
      {
        id: id,
        from: currentOrder.status,
        to: input.status,
        changedBy: input.changedBy,
      },
      `📋 Order status updated: ${currentOrder.status} → ${input.status} (event written to outbox)`
    );

    return updatedOrder;
  }

  // ─────────────────────────────────────────────────
  // CANCEL ORDER
  // ─────────────────────────────────────────────────
  async cancelOrder(id: string, reason?: string): Promise<Order> {
    return this.updateOrderStatus(id, {
      status: "CANCELLED",
      changedBy: "customer",
      reason: reason ?? "Cancelled by customer",
    });
  }

  // ─────────────────────────────────────────────────
  // GET STATUS HISTORY
  // ─────────────────────────────────────────────────
  async getOrderStatusHistory(orderId: string): Promise<OrderStatusHistoryEntry[]> {
    const order = await this.orderRepo.findById(orderId);
    if (!order) {
      throw new NotFoundError('Order', orderId);
    }

    return this.orderRepo.getStatusHistory(orderId);
  }

  // ─────────────────────────────────────────────────
  // Delivery fee calculation
  // ─────────────────────────────────────────────────
  private calculateDeliveryFee(orderAmount: number): number {
    return orderAmount >= 30 ? 0 : 5;
  }
}
