CREATE OR REPLACE TABLE integral_sum_values (
    timestamp TIMESTAMP_NTZ,
    cumulative_sum FLOAT,
    integral FLOAT,
    delta FLOAT,
    value FLOAT
);
COMMENT ON TABLE integral_sum_values IS 'Integral sum table populated periodically by ETL tasks';