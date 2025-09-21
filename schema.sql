-- schema.sql — Fecom Inc E-commerce (PostgreSQL)
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS order_payments;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS geolocations;

-- Геолокации (многие строки на один индекс, без PK)
CREATE TABLE geolocations (
  geo_postal_code        TEXT,
  geo_lat                DOUBLE PRECISION,
  geo_lon                DOUBLE PRECISION,
  geolocation_city       TEXT,
  geo_country            TEXT
);

-- Покупатели
CREATE TABLE customers (
  customer_trx_id        TEXT PRIMARY KEY,
  subscriber_id          TEXT,
  subscribe_date         TIMESTAMP NULL,
  first_order_date       TIMESTAMP NULL,
  customer_postal_code   TEXT,
  customer_city          TEXT,
  customer_country       TEXT,
  customer_country_code  TEXT,
  age                    INTEGER,
  gender                 TEXT
);

-- Продавцы
CREATE TABLE sellers (
  seller_id              TEXT PRIMARY KEY,
  seller_name            TEXT,
  seller_postal_code     TEXT,
  seller_city            TEXT,
  country_code           TEXT,
  seller_country         TEXT
);

-- Товары
CREATE TABLE products (
  product_id                 TEXT PRIMARY KEY,
  product_category           TEXT,
  product_name_length_chars  INTEGER,
  product_description_length_chars INTEGER,
  product_photos_qty         INTEGER,
  product_weight_g           INTEGER,
  product_length_cm          DOUBLE PRECISION,
  product_height_cm          DOUBLE PRECISION,
  product_width_cm           DOUBLE PRECISION
);

-- Заказы
CREATE TABLE orders (
  order_id                        TEXT PRIMARY KEY,
  customer_trx_id                 TEXT REFERENCES customers(customer_trx_id),
  order_status                    TEXT,
  order_purchase_timestamp        TIMESTAMP NULL,
  order_approved_at               TIMESTAMP NULL,
  order_delivered_carrier_date    TIMESTAMP NULL,
  order_delivered_customer_date   TIMESTAMP NULL,
  order_estimated_delivery_date   TIMESTAMP NULL
);

-- Позиции заказа (составной PK, т.к. Order_Item_ID повторяется в каждом заказе)
CREATE TABLE order_items (
  order_id              TEXT REFERENCES orders(order_id),
  order_item_id         INTEGER,
  product_id            TEXT REFERENCES products(product_id),
  seller_id             TEXT REFERENCES sellers(seller_id),
  shipping_limit_date   TIMESTAMP NULL,
  price                 NUMERIC(12,2),
  freight_value         NUMERIC(12,2),
  PRIMARY KEY (order_id, order_item_id)
);

-- Платежи (составной PK)
CREATE TABLE order_payments (
  order_id             TEXT REFERENCES orders(order_id),
  payment_sequential   INTEGER,
  payment_type         TEXT,
  payment_installments INTEGER,
  payment_value        NUMERIC(12,2),
  PRIMARY KEY (order_id, payment_sequential)
);

-- Отзывы
CREATE TABLE order_reviews (
  review_id                  TEXT PRIMARY KEY,
  order_id                   TEXT REFERENCES orders(order_id),
  review_score               INTEGER,
  review_comment_title_en    TEXT,
  review_comment_message_en  TEXT,
  review_creation_date       TIMESTAMP NULL,
  review_answer_timestamp    TIMESTAMP NULL
);

-- (Опционально) Индексы для ускорения аналитики
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_trx_id);
CREATE INDEX IF NOT EXISTS idx_items_product   ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_items_seller    ON order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_items_order     ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_order  ON order_payments(order_id);
