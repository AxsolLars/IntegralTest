from datetime import datetime, timedelta, date, time, timezone
import random
from scripts.connectToSnowflake import connect_to_snowflake
def seed_integral_sum_values(start_date: date, end_date: date, start_time: time, end_time: time, update_delta: int, gap_start: time = None, gap_end: time = None, gap_value: float = -1):
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    print("Connected to Snowflake")
    try:
        rows_to_insert = []
        cumulative_sum = 0
        base_value = 0
        while start_date <= end_date:
            curr_datetime = datetime.combine(date=start_date, time=start_time, tzinfo= timezone.utc)
            end_datetime = datetime.combine(date=start_date, time=end_time, tzinfo= timezone.utc)
            if gap_value == -1:
                gap_start_datetime = datetime.combine(date=start_date, time=end_time, tzinfo=timezone.utc)
                gap_end_datetime = datetime.combine(date=start_date, time=start_time, tzinfo=timezone.utc)
            else:
                gap_start_datetime = datetime.combine(date=start_date, time=gap_start, tzinfo=timezone.utc)
                gap_end_datetime = datetime.combine(date=start_date, time=gap_end, tzinfo=timezone.utc)
            print(curr_datetime, end_datetime)
            while curr_datetime <= end_datetime:
                noise = random.uniform(0.0, 0.0028)
                if curr_datetime >= gap_start_datetime and curr_datetime <= gap_end_datetime:
                    integral_value = gap_value
                else:
                    integral_value = base_value + noise 
                cumulative_sum += integral_value
                rows_to_insert.append((curr_datetime, cumulative_sum, integral_value, update_delta / 1000, 0))
                
        
                curr_datetime += timedelta(milliseconds=update_delta)
            
            start_date += timedelta(days=1)


        batch_size = 40000
        sql = """
            INSERT INTO INTEGRAL_TEST.INTEGRAL_SUM_VALUES
            (timestamp, cumulative_sum, integral, delta, value)
            VALUES (%s, %s, %s, %s, %s)
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
    seed_integral_sum_values(
        start_date=date(2025, 11, 1),
        end_date=date(2025, 11, 3),
        start_time=time(10, 43, 0),
        end_time=time(11, 43, 0),
        update_delta=1000,
        gap_start=time(11, 00, 0),
        gap_end=time(11, 30, 0),
        gap_value=0.0002
    )

