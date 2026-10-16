// ============================================================
// Order Repository — Database Access Layer
// ============================================================

import { Pool, PoolClient } from "pg";
import {
  CreateOrderInput,
  Order,
  OrderStatusType,
  OrderStatusHistoryEntry,
} from "./order.types.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-repository");

export class OrderRepository {
  constructor(private readonly pool: Pool) { }

  // ─────────────────────────────────────────────────
  // CREATE — Transactional multi-table insert
  // ─────────────────────────────────────────────────
  async create(
    input: CreateOrderInput,
    idempotencyKey: string | undefined,
    calculatedTotals: {
      totalAmount: number;
      deliveryFee: number;
      taxAmount: number;
      grandTotal: number;
    },
    onTransaction?: (client: PoolClient) => Promise<void>
  ): Promise<Order> {
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query("BEGIN");

      if (idempotencyKey) {
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
          idempotencyKey,
        ]);

        const existingOrderResult = await client.query<OrderRow>(
          `SELECT * FROM orders WHERE idempotency_key = $1`,
          [idempotencyKey],
        );

        if (existingOrderResult.rows.length > 0) {
          const existingRow = existingOrderResult.rows[0]!;
          const itemsResult = await client.query<OrderItemRow>(
            `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
            [existingRow.id],
          );

          await client.query("COMMIT");

          log.info(
            { orderId: existingRow.id, idempotencyKey },
            "Idempotency hit during transaction (pg_advisory_xact_lock)",
          );

          return this.mapToOrder(existingRow, itemsResult.rows);
        }
      }

      // Insert Order
      const orderResult = await client.query<OrderRow>(
        `INSERT INTO orders (
                    customer_id, customer_name, customer_email, customer_phone,
                    restaurant_id, restaurant_name,
                    status,
                    total_amount, delivery_fee, tax_amount, grand_total,
                    delivery_address, delivery_lat, delivery_lng,
                    notes, idempotency_key
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7,
                    $8, $9, $10, $11,
                    $12, $13, $14,
                    $15, $16
                ) RETURNING *`,
        [
          input.customerId,
          input.customerName,
          input.customerEmail,
          input.customerPhone ?? null,
          input.restaurantId,
          input.restaurantName,
          "PENDING",
          calculatedTotals.totalAmount,
          calculatedTotals.deliveryFee,
          calculatedTotals.taxAmount,
          calculatedTotals.grandTotal,
          input.deliveryAddress,
          input.deliveryLat ?? null,
          input.deliveryLng ?? null,
          input.notes ?? null,
          idempotencyKey ?? null,
        ],
      );

      const orderRow = orderResult.rows[0]!;

      // Insert order items
      const items: OrderItemRow[] = [];
      for (const item of input.items) {
        const totalPrice = item.quantity * item.unitPrice;
        const itemResult = await client.query<OrderItemRow>(
          `INSERT INTO order_items (
                             order_id, item_name, quantity, unit_price, total_price, customizations 
                         ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
          [
            orderRow.id,
            item.itemName,
            item.quantity,
            item.unitPrice,
            totalPrice,
            item.customizations ?? null,
          ],
        );

        items.push(itemResult.rows[0]!);
      }

      // Insert initial status history
      await client.query(
        `INSERT INTO order_status_history (
                    order_id, from_status, to_status, changed_by, reason
                ) VALUES ($1, NULL, $2, $3, $4)`,
        [orderRow.id, "PENDING", "system", "Order created"],
      );

      // Execute outbox callback within the same transaction
      if (onTransaction) {
        await onTransaction(client);
      }

      await client.query("COMMIT");

      log.info(
        { orderId: orderRow.id, itemCount: items.length },
        "Order created in database",
      );

      return this.mapToOrder(orderRow, items);
    } catch (err) {
      await client.query("ROLLBACK");

      // Handle race condition: check if another concurrent request committed with same idempotency key
      if (
        idempotencyKey &&
        err instanceof Error &&
        "code" in err &&
        err.code === "23505"
      ) {
        log.warn(
          { idempotencyKey },
          "Idempotency conflict (concurrent request won race), fetching existing order",
        );

        const existing = await this.findByIdempotencyKey(idempotencyKey);
        if (existing) {
          return existing;
        }
      }

      throw err;
    } finally {
      client.release();
    }
  }

  // ─────────────────────────────────────────────────
  // READ — Single order by ID with its items
  // ─────────────────────────────────────────────────
  async findById(id: string): Promise<Order | null> {
    const orderResult = await this.pool.query<OrderRow>(
      `SELECT * FROM orders WHERE id = $1`,
      [id],
    );

    if (orderResult.rows.length === 0) {
      return null;
    }

    const itemResult = await this.pool.query<OrderItemRow>(
      `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
      [id],
    );

    return this.mapToOrder(orderResult.rows[0]!, itemResult.rows);
  }

  // ─────────────────────────────────────────────────
  // READ — By idempotency key
  // ─────────────────────────────────────────────────
  async findByIdempotencyKey(key: string): Promise<Order | null> {
    const orderResult = await this.pool.query<OrderRow>(
      `SELECT * FROM orders WHERE idempotency_key = $1`,
      [key],
    );

    if (orderResult.rows.length === 0) {
      return null;
    }

    const itemResult = await this.pool.query<OrderItemRow>(
      `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
      [orderResult.rows[0]!.id],
    );

    return this.mapToOrder(orderResult.rows[0]!, itemResult.rows);
  }

  // ─────────────────────────────────────────────────
  // LIST (Paginated)
  // ─────────────────────────────────────────────────
  async findAll(
    page: number,
    limit: number,
    filters?: { status?: OrderStatusType; customerId?: string },
  ): Promise<{ orders: Order[]; total: number }> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIndex = 1;

    if (filters?.status) {
      conditions.push(`status=$${paramIndex++}`);
      params.push(filters.status);
    }

    if (filters?.customerId) {
      conditions.push(`customer_id=$${paramIndex++}`);
      params.push(filters.customerId);
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const countResult = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) FROM orders ${whereClause}`,
      params,
    );

    const total = parseInt(countResult.rows[0]!.count, 10);

    const offset = (page - 1) * limit;
    const orderResult = await this.pool.query<OrderRow>(
      `SELECT * FROM orders ${whereClause}
         ORDER BY created_at DESC
         LIMIT $${paramIndex++} OFFSET $${paramIndex}`,
      [...params, limit, offset],
    );

    const orderIds = orderResult.rows.map((r) => r.id);
    let itemsByOrderId: Map<string, OrderItemRow[]> = new Map();

    if (orderIds.length > 0) {
      const itemResult = await this.pool.query<OrderItemRow>(
        `SELECT * FROM order_items WHERE order_id = ANY($1) ORDER BY created_at`,
        [orderIds],
      );

      for (const item of itemResult.rows) {
        const existing = itemsByOrderId.get(item.order_id) ?? [];
        existing.push(item);
        itemsByOrderId.set(item.order_id, existing);
      }
    }

    const orders = orderResult.rows.map((row) =>
      this.mapToOrder(row, itemsByOrderId.get(row.id) ?? []),
    );

    return { orders, total };
  }

  // ─────────────────────────────────────────────────
  // UPDATE STATUS
  // ─────────────────────────────────────────────────
  async updateStatus(
    id: string,
    fromStatus: OrderStatusType,
    toStatus: OrderStatusType,
    changedBy: string,
    reason?: string,
    onTransaction?: (client: PoolClient) => Promise<void>
  ): Promise<Order | null> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      // Optimistic concurrency control: ensure status matches fromStatus
      const result = await client.query<OrderRow>(
        `UPDATE orders SET status = $1
        WHERE id = $2 AND status = $3
        RETURNING *
        `,
        [toStatus, id, fromStatus],
      );

      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        return null;
      }

      // Record status change history
      await client.query(
        `INSERT INTO order_status_history (
            order_id, from_status, to_status, changed_by, reason
        ) VALUES ($1, $2, $3, $4, $5)`,
        [id, fromStatus, toStatus, changedBy, reason ?? null],
      );

      // Execute outbox callback within the same transaction
      if (onTransaction) {
        await onTransaction(client);
      }

      const itemsResult = await client.query<OrderItemRow>(
        `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
        [id],
      );

      await client.query("COMMIT");

      return this.mapToOrder(result.rows[0]!, itemsResult.rows);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async getStatusHistory(orderId: string): Promise<OrderStatusHistoryEntry[]> {
    const result = await this.pool.query<StatusHistoryRow>(
      `SELECT * from order_status_history where order_id = $1
        ORDER BY created_at ASC
        `,
      [orderId],
    );

    return result.rows.map((row) => ({
      id: row.id,
      orderId: row.order_id,
      fromStatus: row.from_status,
      toStatus: row.to_status,
      changedBy: row.changed_by,
      reason: row.reason ?? undefined,
      metadata: row.metadata ?? undefined,
      createdAt: row.created_at,
    }));
  }

  // ─────────────────────────────────────────────────
  // Row-to-Domain mapping
  // ─────────────────────────────────────────────────

  private mapToOrder(row: OrderRow, itemRows: OrderItemRow[]): Order {
    return {
      id: row.id,
      status: row.status as OrderStatusType,

      customerId: row.customer_id,
      customerName: row.customer_name,
      customerEmail: row.customer_email,
      customerPhone: row.customer_phone ?? undefined,

      restaurantId: row.restaurant_id,
      restaurantName: row.restaurant_name,

      totalAmount: parseFloat(row.total_amount),
      deliveryFee: parseFloat(row.delivery_fee),
      taxAmount: parseFloat(row.tax_amount),
      grandTotal: parseFloat(row.grand_total),

      deliveryAddress: row.delivery_address,
      deliveryLat:
        row.delivery_lat !== null ? parseFloat(row.delivery_lat) : undefined,
      deliveryLng:
        row.delivery_lng !== null ? parseFloat(row.delivery_lng) : undefined,
      notes: row.notes ?? undefined,
      idempotencyKey: row.idempotency_key ?? undefined,
      items: itemRows.map((item) => ({
        id: item.id,
        orderId: item.order_id,
        itemName: item.item_name,
        quantity: item.quantity,
        unitPrice: parseFloat(item.unit_price),
        totalPrice: parseFloat(item.total_price),
        customizations: item.customizations ?? undefined,
        createdAt: item.created_at,
      })),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }
}

interface OrderRow {
  id: string;
  status: string;

  customer_id: string;
  customer_name: string;
  customer_email: string;
  customer_phone: string | null;

  restaurant_id: string;
  restaurant_name: string;

  total_amount: string;
  delivery_fee: string;
  tax_amount: string;
  grand_total: string;

  delivery_address: string;
  delivery_lat: string | null;
  delivery_lng: string | null;
  notes: string | null;

  idempotency_key: string | null;
  created_at: Date;
  updated_at: Date;
}

interface OrderItemRow {
  id: string;
  order_id: string;

  item_name: string;
  quantity: number;
  unit_price: string;
  total_price: string;
  customizations: string | null;
  created_at: Date;
}

interface StatusHistoryRow {
  id: string;
  order_id: string;
  from_status: OrderStatusType | null;
  to_status: OrderStatusType;
  changed_by: string;
  reason: string;
  metadata: Record<string, unknown> | null;
  created_at: Date;
}
