-- Random Values --
SELECT *
FROM random_values
ORDER BY id DESC;
-- Stream --
SELECT *
FROM random_values_stream
ORDER BY timestamp DESC;
-- Task --
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'integrate_stream_task'
))
ORDER BY SCHEDULED_TIME DESC;

-- Resulting Table --
SELECT *
FROM integral_sum_values
ORDER BY timestamp DESC;
-- Stream Marker --
SELECT *
FROM stream_consumed_marker
ORDER BY processed_at DESC;