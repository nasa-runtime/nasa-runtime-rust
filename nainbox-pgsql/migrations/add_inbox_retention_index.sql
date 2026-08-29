CREATE INDEX IF NOT EXISTS inbox_message_retention_idx
    ON inbox_message (consumer_name, processed_at_ms);
