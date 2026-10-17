-- StreamForge Multi-Tenant Streaming CDC Pipeline Schema
DROP TABLE IF EXISTS warehouse_partitions CASCADE;
DROP TABLE IF EXISTS cdc_mutation_events CASCADE;
DROP TABLE IF EXISTS cdc_source_tables CASCADE;
DROP TABLE IF EXISTS tenants CASCADE;

CREATE TABLE tenants (
    tenant_id VARCHAR(32) PRIMARY KEY,
    organization_name VARCHAR(128) NOT NULL,
    tier VARCHAR(32) DEFAULT 'ENTERPRISE',
    cdc_stream_topic VARCHAR(64) NOT NULL,
    status VARCHAR(32) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE cdc_source_tables (
    table_id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(32) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    database_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    primary_key_col VARCHAR(64) NOT NULL,
    schema_version INT DEFAULT 1
);

CREATE TABLE cdc_mutation_events (
    event_id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(32) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    table_name VARCHAR(64) NOT NULL,
    op_type VARCHAR(16) NOT NULL, -- INSERT, UPDATE, DELETE
    record_pk VARCHAR(64) NOT NULL,
    payload_json TEXT NOT NULL,
    lsn_offset BIGINT NOT NULL,
    partition_id INT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE warehouse_partitions (
    partition_id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(32) NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    partition_key VARCHAR(64) NOT NULL,
    record_count BIGINT DEFAULT 0,
    size_bytes BIGINT DEFAULT 0,
    compacted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_cdc_tenant_table ON cdc_mutation_events(tenant_id, table_name);
CREATE INDEX idx_cdc_lsn ON cdc_mutation_events(lsn_offset);
CREATE INDEX idx_cdc_record_pk ON cdc_mutation_events(record_pk);
CREATE INDEX idx_warehouse_tenant_key ON warehouse_partitions(tenant_id, partition_key);

-- Seed Initial Tenants and Tables
INSERT INTO tenants (tenant_id, organization_name, tier, cdc_stream_topic) VALUES
('TENANT_STRIPE_PAYMENTS', 'Stripe Global Payments Corp', 'TIER_1_SCALE', 'cdc.stripe.mutations'),
('TENANT_SHOPIFY_STORE', 'Shopify E-Commerce Cloud', 'TIER_1_SCALE', 'cdc.shopify.mutations'),
('TENANT_AIRBNB_BOOKINGS', 'Airbnb Hospitality Mesh', 'TIER_2_GROWTH', 'cdc.airbnb.mutations');

INSERT INTO cdc_source_tables (tenant_id, database_name, table_name, primary_key_col, schema_version) VALUES
('TENANT_STRIPE_PAYMENTS', 'stripe_prod', 'charges', 'charge_id', 3),
('TENANT_STRIPE_PAYMENTS', 'stripe_prod', 'refunds', 'refund_id', 2),
('TENANT_SHOPIFY_STORE', 'shopify_orders', 'orders', 'order_id', 4),
('TENANT_AIRBNB_BOOKINGS', 'airbnb_reservations', 'bookings', 'booking_id', 1);

INSERT INTO warehouse_partitions (tenant_id, partition_key, record_count, size_bytes) VALUES
('TENANT_STRIPE_PAYMENTS', 'date=2026-10-17', 2500000, 154000000),
('TENANT_SHOPIFY_STORE', 'date=2026-10-17', 1800000, 98000000),
('TENANT_AIRBNB_BOOKINGS', 'date=2026-10-17', 650000, 42000000);

SELECT setval('cdc_mutation_events_event_id_seq', 1);
