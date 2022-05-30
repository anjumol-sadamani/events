BEGIN;

CREATE TABLE IF NOT EXISTS parsed_schema_events (
    id SERIAL PRIMARY KEY,
    schema_path text
);

COMMIT;