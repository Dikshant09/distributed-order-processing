-- ============================================================
-- Database Schema: Order Platform
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- ENUM TYPES
-- ============================================================

CREATE TYPE order_status AS ENUM (
  'PENDING',
  'CONFIRMED',
  'PREPARING',
  'READY_FOR_PICKUP',
  'OUT_FOR_DELIVERY',
  'DELIVERED',
  'CANCELLED'
);

CREATE TYPE notification_type AS ENUM (
  'ORDER_CONFIRMED',
  'ORDER_STATUS_CHANGED',
  'ORDER_CANCELLED',
  'DELIVERY_UPDATE'
);

CREATE TYPE notification_channel AS ENUM (
  'EMAIL',
  'SMS',
  'PUSH'
);

CREATE TYPE notification_status AS ENUM (
  'PENDING',
  'SENT',
  'FAILED'
);

-- ============================================================
-- ORDERS TABLE
-- ============================================================
CREATE TABLE orders (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  
  -- Customer info
  customer_id     VARCHAR(255) NOT NULL,
  customer_name   VARCHAR(255) NOT NULL,
  customer_email  VARCHAR(255) NOT NULL,
  customer_phone  VARCHAR(50),
  
  -- Restaurant info
  restaurant_id   VARCHAR(255) NOT NULL,
  restaurant_name VARCHAR(255) NOT NULL,
  
  -- Order state
  status          order_status NOT NULL DEFAULT 'PENDING',
  
  -- Financial fields
  total_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0,
  delivery_fee    NUMERIC(12, 2) NOT NULL DEFAULT 0,
  tax_amount      NUMERIC(12, 2) NOT NULL DEFAULT 0,
  grand_total     NUMERIC(12, 2) NOT NULL DEFAULT 0,
  
  -- Delivery info
  delivery_address TEXT NOT NULL,
  delivery_lat     NUMERIC(10, 7),
  delivery_lng     NUMERIC(10, 7),
  
  -- Metadata
  notes           TEXT,
  idempotency_key VARCHAR(255) UNIQUE,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ORDER ITEMS TABLE
-- ============================================================
CREATE TABLE order_items (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  item_name       VARCHAR(255) NOT NULL,
  quantity        INTEGER NOT NULL CHECK (quantity > 0),
  unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
  total_price     NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0),
  customizations  TEXT,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ORDER STATUS HISTORY
-- ============================================================
-- Append-only audit log for order status transitions
CREATE TABLE order_status_history (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  from_status     order_status,
  to_status       order_status NOT NULL,
  changed_by      VARCHAR(255) NOT NULL DEFAULT 'system',
  reason          TEXT,
  metadata        JSONB DEFAULT '{}',
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- NOTIFICATIONS TABLE
-- ============================================================
CREATE TABLE notifications (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  type            notification_type NOT NULL,
  channel         notification_channel NOT NULL,
  status          notification_status NOT NULL DEFAULT 'PENDING',
  
  recipient       VARCHAR(255) NOT NULL,
  subject         VARCHAR(500),
  content         TEXT NOT NULL,
  
  retry_count     INTEGER NOT NULL DEFAULT 0,
  failure_reason  TEXT,
  
  sent_at         TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- OUTBOX EVENTS TABLE (Transactional Outbox Pattern)
-- ============================================================
CREATE TABLE outbox_events (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  aggregate_id  VARCHAR(255) NOT NULL,
  event_type    VARCHAR(100) NOT NULL,
  topic         VARCHAR(255) NOT NULL,
  key           VARCHAR(255),
  payload       JSONB NOT NULL,
  headers       JSONB DEFAULT '{}',
  status        VARCHAR(50) DEFAULT 'PENDING',
  locked_at     TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at  TIMESTAMPTZ
);

-- ============================================================
-- PROCESSED EVENTS TABLE (Consumer Idempotency)
-- ============================================================
CREATE TABLE processed_events (
  event_id        VARCHAR(255) NOT NULL,
  consumer_group  VARCHAR(100) NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_id, consumer_group)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Orders: queried by customer, status, and creation time
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_orders_restaurant_id ON orders(restaurant_id);

-- Order items: queried by order
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

-- Status history: queried by order, newest first
CREATE INDEX idx_order_status_history_order_id ON order_status_history(order_id, created_at DESC);

-- Notifications: queried by order and status
CREATE INDEX idx_notifications_order_id ON notifications(order_id);
CREATE INDEX idx_notifications_status ON notifications(status);

-- Outbox: partial index for fast polling of unpublished events
CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;

-- ============================================================
-- UPDATED_AT TRIGGER
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_orders_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

-- Safeguards for existing schema
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'PENDING';
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;