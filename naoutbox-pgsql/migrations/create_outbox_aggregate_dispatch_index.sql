CREATE INDEX IF NOT EXISTS outbox_aggregate_dispatchable_idx
    ON outbox_event (aggregate_type, aggregate_id, id)
    WHERE NOT dispatched AND NOT dead;
