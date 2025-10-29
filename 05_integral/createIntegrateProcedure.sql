USE ROLE role_test_etl;
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
        
    rows = session.sql("""
            SELECT timestamp, value
            FROM random_values_stream
            ORDER BY timestamp ASC
        """).collect()
    

    if not rows:
        return "no new data"

    

    rows_to_insert = []

    if not prev_timestamp:
        prev_timestamp = rows[0]['TIMESTAMP'] - timedelta(seconds=1) #needs to be changed according to the interval length, maybe hardocde average interval somewhere

    for row in rows:
        curr_timestamp = row['TIMESTAMP']
        curr_value = row['VALUE']

        delta = curr_timestamp - prev_timestamp

        factor = (-1) / 2  *  (delta.total_seconds() / 3600) # hours, averaged height, flipped sign
        integral = (curr_value * (curr_value < 0) + prev_value * (prev_value < 0)) * factor
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