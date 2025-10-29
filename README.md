# ⚙️ Snowflake Stream Integration Project

This project integrates streaming power data from Snowflake using a Python Snowpark stored procedure and scheduled task.
It calculates energy values (kWh) from kW readings and writes cumulative sums into a results table.

---

## 📁 Project Structure

```
10_allTogether/
│
├── createAllTablesNew.sql     # Creates all required tables, stream, and the Python stored procedure
├── showAll.sql                # Contains SQL SELECT statements to inspect tables, streams, and task history
├── taskManager.sql            # Allows pausing and resuming the scheduled task
└── scripts/
    └── generateRandomData.py  # Generates random time-series data for testing
```

---

## 🧠 Requirements

### Using Poetry (recommended)

1. Install Poetry:

   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```
2. Install dependencies:

   ```bash
   poetry install
   ```
3. Run the data generator:

   ```bash
   poetry run python scripts/generateRandomData.py
   ```

### Without Poetry (Linux/macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install snowflake-snowpark-python
python scripts/generateRandomData.py
```

---

## 🧩 Stored Procedure

The stored procedure `integrate_stream()`:

* Reads new rows from the Snowflake stream `random_values_stream`
* Calculates energy integrals
* Appends results to `integral_sum_values`
* Marks stream rows as consumed in `stream_consumed_marker`

---

## 🕒 Scheduled Task

The scheduled task runs the stored procedure every minute.

```sql
CREATE OR REPLACE TASK integrate_stream_task
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = '1 MINUTE'
  CONDITION = WHEN SYSTEM$STREAM_HAS_DATA('RANDOM_VALUES_STREAM')
AS
CALL integrate_stream();
```

Enable or pause it:

```sql
ALTER TASK integrate_stream_task RESUME;
ALTER TASK integrate_stream_task SUSPEND;
```

Check task history:

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'INTEGRATE_STREAM_TASK'
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## 📜 Included SQL Utilities

### `createAllTablesNew.sql`

Creates:

* Source table: `random_values`
* Stream: `random_values_stream`
* Result table: `integral_sum_values`
* Marker table: `stream_consumed_marker`
* Stored procedure: `integrate_stream()`

### `showAll.sql`

Quickly view all important tables and task history:

```sql
-- Random Values
SELECT * FROM random_values ORDER BY id DESC;

-- Stream
SELECT * FROM random_values_stream ORDER BY timestamp DESC;

-- Task History
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'integrate_stream_task'
))
ORDER BY SCHEDULED_TIME DESC;

-- Resulting Integrals
SELECT * FROM integral_sum_values ORDER BY timestamp DESC;

-- Stream Marker
SELECT * FROM stream_consumed_marker ORDER BY processed_at DESC;
```

### `taskManager.sql`

```sql
ALTER TASK integrate_stream_task RESUME;
ALTER TASK integrate_stream_task SUSPEND;
```

---

## ✅ Summary

| File                            | Purpose                                                  |
| ------------------------------- | -------------------------------------------------------- |
| `createAllTablesNew.sql`        | Sets up all database objects (tables, stream, procedure) |
| `showAll.sql`                   | Provides queries to inspect data and task history        |
| `taskManager.sql`               | Controls the task (pause/resume)                         |
| `scripts/generateRandomData.py` | Generates random kW data for testing                     |

---

**Author:** Lars
**Environment:** Snowflake + Python (Snowpark) + Poetry
**License:** Internal / Educational Use
