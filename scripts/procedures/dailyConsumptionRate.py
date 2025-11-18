from snowflake.snowpark import Session
from datetime import date, timedelta
from scripts.connection.connectToSnowflake import create_session
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

    export_base = export_data[0]["BASE"]
    export_end = export_data[0]["END_SUM"] 
    if export_end is None or export_base is None:
        return "No export data for the day"
    
    production_base =production_data[0]["BASE"]
    production_end = production_data[0]["END_SUM"]
    if production_end is None or production_base is None:
        return "No production data for the day"
    
    export_sum = export_end - export_base
    production_sum = production_end - production_base
    
    session.sql(
        """
        MERGE INTO sum_history t
        USING (SELECT ? AS date, ? AS production_sum, ? AS export_sum) s
        ON t.date = s.date
        WHEN MATCHED THEN
            UPDATE SET 
                production_sum = s.production_sum,
                export_sum     = s.export_sum
        WHEN NOT MATCHED THEN
            INSERT (date, production_sum, export_sum)
            VALUES (s.date, s.production_sum, s.export_sum);
        """,
        params=[date, production_sum, export_sum]
    ).collect()
    return "ok"

if __name__ == "__main__":    
    start = date(2025, 10, 21)
    end = date.today() - timedelta(days=1)

    current = start
    session = create_session()
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(run(session, date_str))
        current += timedelta(days=1)
    