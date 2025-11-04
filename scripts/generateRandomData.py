from scripts.connectToSnowflake import connect_to_snowflake
import time, random
from datetime import datetime, timezone

def generate_random_value(low: float = -10, high: float = 10) -> float:
    return random.uniform(low, high)

def insert_random_value(conn, value: float):
    with conn.cursor() as cur:
        timestamp = datetime.now(timezone.utc)
        cur.execute("INSERT INTO random_values(value, timestamp) VALUES (%s, %s)", (value, timestamp,))
        print(f"Inserted value: {value:.2f}, {timestamp}")
        
def main(interval: float = 1.0):
    conn = connect_to_snowflake()
    print("Connected to Snowflake")
    print("Generating random values every", interval, "seconds")
    
    try:
        while True:
            value = generate_random_value()
            insert_random_value(conn, value)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n Stopped by User")
    finally:
        conn.close()
        print("Connection closed")
        
    
if __name__ == "__main__":
    main()