CREATE TABLE inbox_message (
    consumer_name TEXT COLLATE "C" NOT NULL CHECK (octet_length(consumer_name) BETWEEN 1 AND 128),
    message_id TEXT COLLATE "C" NOT NULL CHECK (octet_length(message_id) BETWEEN 1 AND 190),
    processed_at_ms BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT)
        CHECK (processed_at_ms >= 0),
    CONSTRAINT inbox_message_pkey PRIMARY KEY (consumer_name, message_id)
);

CREATE INDEX inbox_message_retention_idx
    ON inbox_message (consumer_name, processed_at_ms);
