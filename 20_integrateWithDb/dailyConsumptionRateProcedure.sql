
CREATE OR REPLACE PROCEDURE sum_history(date DATE)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'

AS
$$
from snowflake.snowpark import Session, Row
from datetime import timedelta
from collections import defaultdict

def run(session: Session, date) -> str:
    export_data = session.sql(f"""
        SELECT 
        MIN_BY(cumulative_sum, timestamp) AS base,
        MAX_BY(cumulative_sum, timestamp) AS end_sum
        FROM power_export_sum
        WHERE DATE(TIMESTAMP) = '{date}'
        """).collect()

    production_data = session.sql(f"""
        SELECT 
        MIN_BY(cumulative_sum, timestamp) AS base,
        MAX_BY(cumulative_sum, timestamp) AS end_sum
        FROM power_production_sum
        WHERE DATE(TIMESTAMP) = '{date}'
        """).collect()

    export_sum = export_data[0]["END_SUM"] - export_data[0]["BASE"]
    production_sum = production_data[0]["END_SUM"] - production_data[0]["BASE"]
    session.sql(f"""
        INSERT INTO sum_history (date, production_sum, export_sum)
        VALUES ('{date}', {production_sum}, {export_sum})
    """).collect()
    return "ok"
$$
