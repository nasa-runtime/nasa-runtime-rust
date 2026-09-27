CREATE TABLE idempotency_record_v2 (
    tenant TEXT COLLATE "C" NOT NULL CHECK (octet_length(tenant) BETWEEN 1 AND 128),
    subject TEXT COLLATE "C" NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 190),
    route_id TEXT COLLATE "C" NOT NULL CHECK (octet_length(route_id) BETWEEN 1 AND 190),
    client_key TEXT COLLATE "C" NOT NULL CHECK (octet_length(client_key) BETWEEN 1 AND 190),
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    lease BYTEA NOT NULL CHECK (octet_length(lease) = 16),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    state SMALLINT NOT NULL CHECK (state IN (0, 1)),
    status INTEGER CHECK (status BETWEEN 100 AND 599),
    body BYTEA,
    headers BYTEA,
    lease_expires_at_ms BIGINT NOT NULL CHECK (lease_expires_at_ms >= 0),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms >= 0),
    CONSTRAINT idempotency_record_v2_pkey PRIMARY KEY (tenant, subject, route_id, client_key)
);
