// ============================================================
// Notification Service
// ============================================================

import { PoolClient } from "pg";
import { config } from "../../config/index.js";
import { createModuleLogger } from "../../shared/logger.js";
import { NotificationRepository } from "./notification.repository.js";
import { SendNotificationInput, Notification } from "./notification.types.js";

const log = createModuleLogger("notification-service");

export class NotificationService {
  constructor(private readonly repository: NotificationRepository) {}

  // ─────────────────────────────────────────────────
  // Send notification
  // ─────────────────────────────────────────────────
  async send(
    input: SendNotificationInput,
    onTransaction?: (client: PoolClient) => Promise<void>,
  ): Promise<Notification> {
    const startTime = Date.now();

    const notification = await this.repository.create(input, onTransaction);

    try {
      await this.simulateExternalApiCall();

      await this.repository.updateStatus(notification.id, "SENT");

      const latency = Date.now() - startTime;
      log.info(
        {
          notificationId: notification.id,
          orderId: input.orderId,
          channel: input.channel,
          type: input.type,
          latencyMs: latency,
        },
        `✉️ Notification sent (${input.channel})`,
      );

      return { ...notification, status: "SENT", sentAt: new Date() };
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error";

      await this.repository.updateStatus(notification.id, "FAILED", errorMessage);

      const latency = Date.now() - startTime;
      log.error(
        {
          notificationId: notification.id,
          orderId: input.orderId,
          channel: input.channel,
          error: errorMessage,
          latencyMs: latency,
        },
        `❌ Notification failed`,
      );

      throw err;
    }
  }

  // ─────────────────────────────────────────────────
  // Build notification content for order events
  // ─────────────────────────────────────────────────
  buildOrderConfirmationContent(order: {
    id: string;
    customerName: string;
    restaurantName: string;
    grandTotal: number;
  }): { subject: string; content: string } {
    return {
      subject: `Order Confirmed - #${order.id.slice(0, 8)}`,
      content: [
        `Hi ${order.customerName},`,
        `Your order from ${order.restaurantName} has been received!`,
        `Order ID: ${order.id}`,
        `Total: $${order.grandTotal.toFixed(2)}`,
        "",
        "We will notify you when your order is being prepared.",
        "",
        "Thank you for your order!",
      ].join("\n"),
    };
  }

  buildOrderUpdateContent(order: {
    id: string;
    customerName: string;
    status: string;
  }): { subject: string; content: string } {
    const statusMessages: Record<string, string> = {
      CONFIRMED: "Your order has been confirmed by the restaurant!",
      PREPARING: "Your order is being prepared!",
      READY_FOR_PICKUP:
        "Your order is ready for pickup by the delivery partner!",
      OUT_FOR_DELIVERY: "Your order is out for delivery!",
      DELIVERED: "Your order has been delivered. Enjoy your meal!",
      CANCELLED: "Your order has been cancelled.",
    };

    return {
      subject: `Order Update - #${order.id.slice(0, 8)} - ${order.status}`,
      content: [
        `Hi ${order.customerName},`,
        '',
        statusMessages[order.status] ?? `Your order status is now: ${order.status}`,
        `Order ID: ${order.id}`,
        '',
        'Thank you!',
      ].join('\n'),
    };
  }

  private async simulateExternalApiCall() {
    if (config.notificationDelayMs > 0) {
      await new Promise((resolve) =>
        setTimeout(resolve, config.notificationDelayMs),
      );
    }

    if (config.notificationFailureRate > 0) {
      if (Math.random() < config.notificationFailureRate) {
        throw new Error(
          "External notification API failed (simulated): Connection timeout to email service",
        );
      }
    }
  }
}
