CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE pg_sync_outbox (
    id BIGSERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    unit_id INT NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    operation CHAR(1) NOT NULL, -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending', -- pending, sent, error
    attempts INT DEFAULT 0,
    error_log TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_status_pending ON pg_sync_outbox(status) WHERE status = 'pending';
CREATE INDEX idx_outbox_created_at ON pg_sync_outbox(created_at);

CREATE TABLE pg_sync_dlq (
    id BIGSERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL,
    unit_id INT NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    operation CHAR(1) NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,
    error_log TEXT,
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_processing_stale 
ON pg_sync_outbox(status, updated_at) 
WHERE status = 'processing';

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    fb_id INT, -- Legacy Firebird ID
    unit_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    document VARCHAR(20) UNIQUE NOT NULL, -- CPF/CNPJ
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE contacts (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id) ON DELETE CASCADE,
    fb_id INT,
    type VARCHAR(20), -- Mobile, Email, Phone
    value VARCHAR(100) NOT NULL
);

CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id) ON DELETE CASCADE,
    fb_id INT,
    street VARCHAR(150) NOT NULL,
    number VARCHAR(10),
    neighborhood VARCHAR(50),
    city VARCHAR(50),
    state CHAR(2)
);

CREATE TABLE contracts (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    fb_id INT,
    plan_id INT, 
    contract_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE installments (
    id SERIAL PRIMARY KEY,
    contract_id INT REFERENCES contracts(id),
    fb_id INT,
    installment_number INT NOT NULL,
    installment_value DECIMAL(10,2) NOT NULL,
    due_date DATE NOT NULL,
    payment_date TIMESTAMP WITH TIME ZONE,
    paid_amount DECIMAL(10,2) DEFAULT 0.00,
    payment_status VARCHAR(20) DEFAULT 'pending'
);