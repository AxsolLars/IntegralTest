from datetime import datetime, timedelta, date, time, timezone
import random
from scripts.connection.connectToSnowflake import connect_to_testing
def seed_random_values_with_gap(start_date: date, end_date: date, start_time: time, end_time: time, update_delta: int, gap_start: time, gap_end: time):
    conn = connect_to_testing()
    cursor = conn.cursor()
    print("Connected to Snowflake")
    try:
        rows_to_insert = []
        while start_date <= end_date:
            curr_datetime = datetime.combine(date=start_date, time=start_time, tzinfo= timezone.utc)
            end_datetime = datetime.combine(date=start_date, time=end_time, tzinfo= timezone.utc)
            print(curr_datetime, end_datetime)
            while curr_datetime <= end_datetime:
                curr_time = curr_datetime.time()
                if curr_time < gap_start or curr_time > gap_end:
                    rows_to_insert.append((curr_datetime, random.uniform(-10, 10)))
                curr_datetime += timedelta(milliseconds=update_delta)
            
            start_date += timedelta(days=1)


        batch_size = 40000
        sql = """
            INSERT INTO INTEGRAL_TEST.RANDOM_VALUES
            (timestamp, value)
            VALUES (%s, %s)
        """

        for i in range(0, len(rows_to_insert), batch_size):
            chunk = rows_to_insert[i:i + batch_size]
            cursor.executemany(sql, chunk)
            conn.commit()
            print(f"Inserted {len(chunk)} rows (total so far: {i + len(chunk)})")
    finally:
        cursor.close()
        conn.close()
        print("Connection closed.")
        
if __name__ == "__main__":
    seed_random_values_with_gap(
        start_date=date(2025, 11, 4),
        end_date=date(2025, 11, 4),
        start_time=time(10, 43, 0),
        end_time=time(11, 43, 0),
        update_delta=1000,
        gap_start =time(11, 00, 0),
        gap_end =time(11, 30, 0)
    )