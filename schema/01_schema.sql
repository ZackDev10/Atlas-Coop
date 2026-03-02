-- Schema definition for Atlas Cooperative - Excel to PostgreSQL
CREATE SCHEMA IF NOT EXISTS atlas;

-- Customers
CREATE TABLE IF NOT EXISTS atlas.customers (
  id            BIGSERIAL PRIMARY KEY,
  customer_code TEXT NOT NULL UNIQUE,
  full_name     TEXT NOT NULL,
  phone         TEXT NULL,
  city          TEXT NULL,
  created_at    DATE NOT NULL
);

-- Index for customers by city
CREATE INDEX IF NOT EXISTS idx_cust_city ON atlas.customers(city);

-- Products
CREATE TABLE IF NOT EXISTS atlas.products (
  id            BIGSERIAL PRIMARY KEY,
  product_code  TEXT NOT NULL UNIQUE,
  product_name  TEXT NOT NULL,
  category      TEXT NOT NULL,
  unit_price    NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
  stock_qty     INT NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
  active        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for products
CREATE INDEX IF NOT EXISTS idx_products_category ON atlas.products(category);
CREATE INDEX IF NOT EXISTS idx_products_active ON atlas.products(active);

-- Orders (header)
CREATE TYPE atlas.order_status AS ENUM ('PENDING','PAID','CANCELLED');

CREATE TABLE IF NOT EXISTS atlas.orders (
  id          BIGSERIAL PRIMARY KEY,
  order_ref   TEXT NOT NULL UNIQUE,
  customer_id BIGINT NOT NULL REFERENCES atlas.customers(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  order_date  DATE NOT NULL,
  status      atlas.order_status NOT NULL DEFAULT 'PENDING',
  created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for orders
CREATE INDEX IF NOT EXISTS idx_orders_customer ON atlas.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON atlas.orders(order_date);

-- Order items
CREATE TABLE IF NOT EXISTS atlas.order_items (
  id         BIGSERIAL PRIMARY KEY,
  order_id   BIGINT NOT NULL REFERENCES atlas.orders(id) ON UPDATE CASCADE ON DELETE CASCADE,
  product_id BIGINT NOT NULL REFERENCES atlas.products(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  qty        INT NOT NULL CHECK (qty > 0),
  unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
  line_total NUMERIC(12,2) GENERATED ALWAYS AS (qty * unit_price) STORED,
  UNIQUE (order_id, product_id)
);

-- Indexes for order items
CREATE INDEX IF NOT EXISTS idx_items_order ON atlas.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product ON atlas.order_items(product_id);

-- Payments
CREATE TYPE atlas.payment_method AS ENUM ('CASH','CARD','BANK_TRANSFER');

CREATE TABLE IF NOT EXISTS atlas.payments (
  id       BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES atlas.orders(id) ON UPDATE CASCADE ON DELETE CASCADE,
  method   atlas.payment_method NOT NULL,
  amount   NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  currency CHAR(3) NOT NULL DEFAULT 'MAD',
  paid_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for payments
CREATE INDEX IF NOT EXISTS idx_payments_order ON atlas.payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_method ON atlas.payments(method);

-- RAW staging tables for initial data load from Excel (before validation and transformation)
CREATE TABLE IF NOT EXISTS atlas.stg_customers (
  customer_code TEXT,
  full_name     TEXT,
  phone         TEXT,
  city          TEXT,
  created_at    TEXT
);

CREATE TABLE IF NOT EXISTS atlas.stg_products (
  product_code  TEXT,
  product_name  TEXT,
  category      TEXT,
  unit_price    TEXT,
  stock_qty     TEXT,
  active        TEXT
);

CREATE TABLE IF NOT EXISTS atlas.stg_orders (
  order_ref       TEXT,
  order_date      TEXT,
  customer_code   TEXT,
  product_code    TEXT,
  qty             TEXT,
  payment_method  TEXT,
  paid_amount     TEXT,
  currency        TEXT
);

-- Rejected rows (for audit + report)
CREATE TABLE IF NOT EXISTS atlas.rejected_rows (
  id          BIGSERIAL PRIMARY KEY,
  source_sheet TEXT NOT NULL,
  row_data     JSONB NOT NULL,
  reason       TEXT NOT NULL,
  rejected_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
