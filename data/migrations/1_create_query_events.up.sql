BEGIN;

CREATE TABLE IF NOT EXISTS query_events (
    id SERIAL PRIMARY KEY,
    client varchar(255),
    client_version varchar(255),
    data_center varchar(255),
    processed_time timestamp,
    query JSONB );

COMMIT;