SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 FROM POWERMETER_STREAM ORDER BY TIMESTAMP DESC;
SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 FROM POWERMETER_STREAM WHERE POWER_REAL_SUM_L1_L2_L3 < 0 ORDER BY TIMESTAMP DESC;

SELECT
    timestamp AS current_time,
    LAG(timestamp) OVER (ORDER BY timestamp) AS previous_time,
    DATEDIFF('second', LAG(timestamp) OVER (ORDER BY timestamp), timestamp) AS diff_seconds
FROM POWERMETER_STREAM
QUALIFY diff_seconds > 60
ORDER BY timestamp;

SELECT
    timestamp AS current_time,
    LAG(timestamp) OVER (ORDER BY timestamp) AS previous_time,
    DATEDIFF('second', LAG(timestamp) OVER (ORDER BY timestamp), timestamp) AS diff_seconds
FROM POWER_PRODUCTION_STREAM
QUALIFY diff_seconds > 1
ORDER BY timestamp;
SHOW QUERIES;
SELECT
  timestamp,
  LAG(timestamp) OVER (ORDER BY timestamp) AS previous_timestamp
FROM POWER_PRODUCTION_STREAM;

SELECT * FROM power_export_sum ORDER BY timestamp DESC;
SELECT * FROM power_export_sum ORDER BY timestamp;
SELECT * FROM power_export_sum WHERE VALUE < 0 ORDER BY timestamp;
SELECT * FROM power_export_sum WHERE CUMULATIVE_SUM < 0 ORDER BY timestamp;
SELECT * FROM power_export_sum WHERE DELTA < 0 ORDER BY timestamp;
-- should be empty--
SELECT 
    curr.timestamp,
    curr.cumulative_sum,
    DATEDIFF('second', LAG(curr.timestamp) OVER (ORDER BY curr.timestamp), curr.timestamp) AS diff_seconds
FROM power_export_sum AS curr
QUALIFY diff_seconds > 60
ORDER BY curr.timestamp;


SELECT * FROM power_production_sum ORDER BY timestamp DESC;
SELECT * FROM power_production_sum ORDER BY timestamp;
SELECT * FROM power_production_sum WHERE VALUE < 0 ORDER BY timestamp;
SELECT * FROM power_production_sum WHERE CUMULATIVE_SUM < 0 ORDER BY timestamp;
SELECT * FROM power_production_sum WHERE DELTA < 0 ORDER BY timestamp;

-- should be empty --
SELECT 
    curr.timestamp,
    curr.cumulative_sum,
    DATEDIFF('second', LAG(curr.timestamp) OVER (ORDER BY curr.timestamp), curr.timestamp) AS diff_seconds
FROM power_production_sum AS curr
QUALIFY diff_seconds > 60
ORDER BY curr.timestamp;