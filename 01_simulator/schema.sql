-- schema.sql

CREATE TABLE customers (
    customer_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL,
    email           VARCHAR(150) UNIQUE NOT NULL,
    phone           VARCHAR(15),
    city            VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE restaurants (
    restaurant_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(150) NOT NULL,
    cuisine_type    VARCHAR(50),
    city            VARCHAR(50),
    avg_rating      NUMERIC(3,2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE menu_items (
    item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    restaurant_id   UUID REFERENCES restaurants(restaurant_id),
    name            VARCHAR(100) NOT NULL,
    category        VARCHAR(50),
    price           NUMERIC(8,2) NOT NULL CHECK (price > 0),
    is_available    BOOLEAN DEFAULT TRUE
);

CREATE TABLE orders (
    order_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     UUID REFERENCES customers(customer_id),
    restaurant_id   UUID REFERENCES restaurants(restaurant_id),
    status          VARCHAR(20) DEFAULT 'placed'
                    CHECK (status IN ('placed','preparing','out_for_delivery','delivered','cancelled')),
    total_amount    NUMERIC(10,2),
    delivery_address TEXT,
    placed_at       TIMESTAMP DEFAULT NOW(),
    delivered_at    TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID REFERENCES orders(order_id),
    item_id         UUID REFERENCES menu_items(item_id),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(8,2) NOT NULL
);

-- Analytics role (Unit 6 security)
CREATE ROLE analytics_reader LOGIN PASSWORD 'readonly123';
GRANT CONNECT ON DATABASE swiggy TO analytics_reader;
GRANT USAGE ON SCHEMA public TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_reader;

-- Reverse ETL target (Unit 6)
CREATE TABLE flagged_orders (
    order_id        UUID PRIMARY KEY,
    flag_reason     VARCHAR(100),
    flagged_at      TIMESTAMP DEFAULT NOW()
);

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
