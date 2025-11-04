DELETE FROM random_values;
DELETE FROM random_values_stream;
INSERT INTO stream_consumed_marker (processed_at, row_count)
            SELECT CURRENT_TIMESTAMP, COUNT(*)
            FROM random_values_stream