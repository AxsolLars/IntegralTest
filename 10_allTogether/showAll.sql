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

-- Gap Testing --

SELECT * 
FROM integral_sum_values
WHERE TO_TIME(TIMESTAMP) 
BETWEEN TO_TIME('11:00:00') AND TO_TIME('11:30:00')
ORDER BY timestamp DESC;
-- Stream Marker --
SELECT *
FROM stream_consumed_marker
ORDER BY processed_at DESC;
SELECT *
FROM INTEGRAL_TEST.INTEGRAL_SUM_VALUES
WHERE TIMESTAMP <= TO_TIMESTAMP_NTZ('11/04/2025 09:20:46')
  AND TIMESTAMP >= TO_TIMESTAMP_NTZ('11/04/2025 09:20:44')
ORDER BY ABS(
    DATE_PART(EPOCH_MILLISECOND, TIMESTAMP)
    - DATE_PART(EPOCH_MILLISECOND, TO_TIMESTAMP_NTZ('11/04/2025 09:20:45'))
)
FETCH FIRST 1 ROWS ONLY;


SELECT
    CAST(TIMESTAMP AS DATE) AS day,
    MIN(TIMESTAMP) AS lower_bound,
    MAX(TIMESTAMP) AS upper_bound
FROM INTEGRAL_TEST.INTEGRAL_SUM_VALUES
WHERE TO_TIME(TIMESTAMP)
      BETWEEN TO_TIME('09:20:00') AND TO_TIME('09:23:00')
GROUP BY CAST(TIMESTAMP AS DATE)
ORDER BY day;


WITH filtered AS (
        SELECT
            CAST(TIMESTAMP AS DATE) AS day,
            TIMESTAMP,
            CUMULATIVE_SUM
        FROM INTEGRAL_TEST.INTEGRAL_SUM_VALUES
        WHERE TO_TIME(TIMESTAMP)
            BETWEEN TO_TIME('09:20:00') AND TO_TIME('09:23:00')
        ),
        bounds AS (
            SELECT
                day,
                MIN(TIMESTAMP) AS lower_bound,
                MAX(TIMESTAMP) AS upper_bound
            FROM filtered
            GROUP BY day
        )
        SELECT
            b.day,
            b.lower_bound,
            lv.CUMULATIVE_SUM AS lower_value,
            b.upper_bound,
            uv.CUMULATIVE_SUM AS upper_value
        FROM bounds b
        JOIN filtered lv
            ON lv.day = b.day AND lv.TIMESTAMP = b.lower_bound
        JOIN filtered uv
            ON uv.day = b.day AND uv.TIMESTAMP = b.upper_bound
        ORDER BY b.day;