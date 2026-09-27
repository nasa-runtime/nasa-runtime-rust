CREATE INDEX inbox_message_retention_idx
    ON inbox_message (consumer_name, processed_at);
