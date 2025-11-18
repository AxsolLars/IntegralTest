SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 FROM POWERMETER_STREAM ORDER BY TIMESTAMP DESC;
SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 FROM POWERMETER_STREAM WHERE POWER_REAL_SUM_L1_L2_L3 < 0 ORDER BY TIMESTAMP DESC;

SELECT TIMESTAMP FROM POWER_PRODUCTION_STREAM ORDER BY TIMESTAMP;
SELECT TIMESTAMP FROM POWERMETER_STREAM ORDER BY TIMESTAMP DESC;
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
SELECT * FROM power_production_sum WHERE TIMESTAMP <= TO_TIMESTAMP_TZ('2025-11-11T12:00:00.816Z') AND TIMESTAMP >= TO_TIMESTAMP_TZ('2025-11-11T11:00:00.827Z') ORDER BY timestamp DESC;
SELECT TIMESTAMP, METER_EZA_1_AC_P as VALUE1, METER_EZA_2_AC_P as VALUE2, METER_EZA_3_AC_P as VALUE3 FROM METEOCONTROL_CTRL WHERE TIMESTAMP <= TO_TIMESTAMP_TZ('2025-11-11T23:00:00.000Z') AND TIMESTAMP >= TO_TIMESTAMP_TZ('2025-11-11T22:00:00.000Z') ORDER BY timestamp DESC;
SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 AS value FROM JANITZA_POWERMETER WHERE TIMESTAMP <= TO_TIMESTAMP_TZ('2025-11-11T23:00:00.000Z') AND TIMESTAMP >= TO_TIMESTAMP_TZ('2025-11-11T22:00:00.000Z') ORDER BY timestamp DESC;
SELECT * FROM power_export_sum WHERE TIMESTAMP <= TO_TIMESTAMP_TZ('2025-11-11T12:00:00.816Z') AND TIMESTAMP >= TO_TIMESTAMP_TZ('2025-11-11T11:00:00.827Z') ORDER BY timestamp DESC;
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


SELECT CURRENT_ACCOUNT();

SHOW TASKS IN SCHEMA axsol_db_dev.bok;
GRANT EXECUTE TASK ON ACCOUNT QG06294 TO ROLE ROLE_AXSOL_ETL;


SELECT *
FROM BOK.POWER_EXPORT_SUM
WHERE TIMESTAMP <= TO_TIMESTAMP_TZ('2025-11-05T12:15:11.8270000+00:00') AND TIMESTAMP >= TO_TIMESTAMP_TZ('2025-11-05T12:13:11.8270000+00:00')
ORDER BY ABS(
DATE_PART(EPOCH_MILLISECOND, TIMESTAMP)
- DATE_PART(EPOCH_MILLISECOND, TO_TIMESTAMP_TZ('2025-11-05T12:14:11.8270000+00:00'))
)
FETCH FIRST 1 ROWS ONLY;


-- tasks --

SELECT 'power_production_task' AS TASK,
       NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'power_production_task'
    )
)
UNION ALL
SELECT 'powermeter_integrate_task' AS TASK,
       NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE 
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'powermeter_integrate_task'
    )
)
UNION ALL
SELECT 'daily_consumption_task' as TASK,
NAME, STATE, SCHEDULED_TIME, ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'daily_consumption_task'
    )
)
ORDER BY SCHEDULED_TIME DESC;

--- all data
SELECT 'power_production_task' AS TASK,
*
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'power_production_task'
    )
)
UNION ALL
SELECT 'powermeter_integrate_task' AS TASK,
*
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'powermeter_integrate_task'
    )
)
UNION ALL
SELECT 'daily_consumption_task' as TASK,
*
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'daily_consumption_task'
    )
)
ORDER BY SCHEDULED_TIME DESC;
SHOW TASKS IN SCHEMA axsol_db_dev.bok;

SHOW GRANTS ON TASK axsol_db_dev.bok.powermeter_integrate_task;

SHOW GRANTS TO ROLE role_axsol_etl;



--- sum_history ---
SELECT * FROM sum_history ORDER BY DATE DESC;
SELECT TIMESTAMP, POWER_REAL_SUM_L1_L2_L3 AS value FROM JANITZA_POWERMETER WHERE TO_DATE(TIMESTAMP) = '2025-11-15' AND VALUE < 0;


SELECT s."Value"
                FROM (
                    SELECT 
                        MAX_BY(cumulative_sum, timestamp) 
                        - MIN_BY(cumulative_sum, timestamp) AS "Value"
                    FROM BOK.POWER_EXPORT_SUM
                    WHERE DATE(TIMESTAMP) = TO_DATE('2025-11-18')
                ) AS s
;

FROM (SELECT 123 AS "Value") s;
