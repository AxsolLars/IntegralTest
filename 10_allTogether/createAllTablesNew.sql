USE DATABASE testing_db;
USE SCHEMA integral_test;

CREATE OR REPLACE TABLE random_values (
    id INT AUTOINCREMENT,
    timestamp TIMESTAMP_NTZ,
    value FLOAT,
    CONSTRAINT pk_random_values PRIMARY KEY (id)
);

COMMENT ON TABLE random_values IS 'Holds live random values inserted by the generator script.';


CREATE OR REPLACE STREAM random_values_stream
ON TABLE random_values;

CREATE OR REPLACE TABLE stream_consumed_marker (
    processed_at TIMESTAMP,
    row_count NUMBER
);

CREATE OR REPLACE TABLE integral_sum_values (
    timestamp TIMESTAMP_NTZ,
    cumulative_sum FLOAT,
    integral FLOAT,
    delta FLOAT,
    value FLOAT
);
ALTER TABLE integral_sum_values CLUSTER BY (timestamp);

COMMENT ON TABLE integral_sum_values IS 'Integral sum table populated periodically by ETL tasks';

CREATE OR REPLACE PROCEDURE integrate_stream()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'

AS
$$
from snowflake.snowpark import Session, Row
from datetime import timedelta

updateTime = 1000
def aggregate(session: Session, curr_timestamp, prev_timestamp, prev_value, last_sum, rows_to_insert: list):
    prev_time = prev_timestamp.time()
    curr_time = curr_timestamp.time()
    window_data = session.sql(f"""
        WITH filtered AS (
        SELECT
            CAST(TIMESTAMP AS DATE) AS day,
            TIMESTAMP,
            CUMULATIVE_SUM
        FROM INTEGRAL_TEST.INTEGRAL_SUM_VALUES
        WHERE TO_TIME(TIMESTAMP)
            BETWEEN TO_TIME('{prev_time}') AND TO_TIME('{curr_time}')
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
            lv.CUMULATIVE_SUM AS lower_value,
            uv.CUMULATIVE_SUM AS upper_value
        FROM bounds b
        JOIN filtered lv
            ON lv.day = b.day AND lv.TIMESTAMP = b.lower_bound
        JOIN filtered uv
            ON uv.day = b.day AND uv.TIMESTAMP = b.upper_bound
        ORDER BY b.day;
    """
    ).collect()
    aggregate_sum = 0
    print(window_data)
    for window in window_data:
        aggregate_sum += window["UPPER_VALUE"] - window["LOWER_VALUE"]
    if len(window_data) == 0:
        return last_sum, 0, curr_timestamp + timedelta(minutes= 1)
    aggregate_sum /= len(window_data)
    time_diff_ms = (curr_timestamp - prev_timestamp).total_seconds() * 1000
    aggregate_integral = aggregate_sum / (time_diff_ms / updateTime)
    while prev_timestamp <= curr_timestamp:
        prev_timestamp += timedelta(milliseconds= updateTime)
        last_sum += aggregate_integral
        rows_to_insert.append(Row(TIMESTAMP=prev_timestamp, CUMULATIVE_SUM=last_sum, INTEGRAL=aggregate_integral, DELTA=updateTime /1000, VALUE=0.0))
    return last_sum, 0, curr_timestamp

def run(session: Session) -> str:
    prev_data = session.sql("""
        SELECT COALESCE(cumulative_sum, 0) AS sum,
        value,
        timestamp
        FROM integral_sum_values
        ORDER BY timestamp DESC
        LIMIT 1
    """).collect()
    last_sum = 0.0
    prev_value = 0.0
    prev_timestamp = None
    if prev_data:
        last_sum = prev_data[0]['SUM']
        prev_value = prev_data[0]['VALUE']
        prev_timestamp = prev_data[0]['TIMESTAMP']
        
    new_values = session.sql("""
            SELECT timestamp, value
            FROM random_values_stream
            ORDER BY timestamp ASC
        """).collect()
    

    if not new_values:
        return "no new data"

    

    rows_to_insert = []

    if not prev_timestamp:
        prev_timestamp = new_values[0]['TIMESTAMP'] - timedelta(milliseconds=updateTime) #needs to be changed according to the interval length, maybe hardocde average interval somewhere

    for new_value_row in new_values:
        curr_timestamp = new_value_row['TIMESTAMP']
        curr_value = new_value_row['VALUE']

        delta = curr_timestamp - prev_timestamp
        if delta.total_seconds() * 1000 > 2 * updateTime:
            last_sum, prev_value, prev_timestamp = aggregate(session, curr_timestamp, prev_timestamp, prev_value, last_sum, rows_to_insert)
            continue
        factor = (-1) / 2  *  (delta.total_seconds() / 3600) # hours, averaged height, flipped sign
        integral = (curr_value * (curr_value < 0) + prev_value * (prev_value < 0)) * factor # curr_value and prev_value gets nulled if if theye >= 0
        last_sum += integral
        prev_value = curr_value
        prev_timestamp = curr_timestamp

        timestamp_string = curr_timestamp.isoformat(sep=' ', timespec='microseconds')
        rows_to_insert.append(Row(TIMESTAMP=curr_timestamp, CUMULATIVE_SUM=last_sum, INTEGRAL=integral, DELTA=delta.total_seconds(), VALUE=curr_value))

    if rows_to_insert:
        df = session.create_dataframe(rows_to_insert)
        df.write.save_as_table("integral_sum_values", mode="append")
        session.sql("""
            INSERT INTO stream_consumed_marker (processed_at, row_count)
            SELECT CURRENT_TIMESTAMP, COUNT(*)
            FROM random_values_stream
        """).collect()
    return "ok"
$$;

CREATE OR REPLACE TASK integrate_stream_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
CALL integrate_stream();