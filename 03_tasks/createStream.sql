USE DATABASE testing_db;
USE SCHEMA integral_test;
CREATE OR REPLACE STREAM random_values_stream
ON TABLE random_values;

CREATE TABLE IF NOT EXISTS stream_consumed_marker (
    processed_at TIMESTAMP,
    row_count NUMBER
);
