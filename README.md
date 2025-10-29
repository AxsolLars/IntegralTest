# ⚙️ Snowflake Stream Integration Project

This project integrates streaming power data from Snowflake using a Python Snowpark stored procedure and scheduled task.
It calculates energy values (kWh) from kW readings and writes cumulative sums into a results table.

---

## 📁 Project Structure

```
00_init/ to 05_integral        # Legacy code showing the development history of the project
10_allTogether/
│
├── createAllTablesNew.sql     # Creates all required tables, stream, and the Python stored procedure
├── showAll.sql                # Contains SQL SELECT statements to inspect tables, streams, and task history
└── taskManager.sql            # Allows pausing and resuming the scheduled task
scripts/
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

### 🔐 Snowflake Connection in VS Code (Private Key Authentication - Recommended)

This guide explains how to connect Visual Studio Code to **Snowflake** using a **private key** and a specific role for authentication.

---

#### 📁 Connection Details

```
[OSCGTUT-OK42230]
account = "OSCGTUT-OK42230"
user = "SVC_TEST_ETL"
authenticator = "SNOWFLAKE_JWT"
private_key_path = "/path/to/your/private_key.pem"
```

> 🔑 The **private key file** must be requested from the **Security-Admin (Amer)**. You will not be able to connect without it.

Replace `/path/to/your/private_key.pem` with the actual path to your private key once provided (for example `/home/<username>/snowflake_keys/rsa_key_private.pem`).

---

#### 🧠 Required Role

Use the following Snowflake role:

```
ROLE = ROLE_TEST_ETL
```

This role should have permissions to run ETL tasks, execute stored procedures, and access relevant schemas and tables.

---

#### ⚙️ Setting Up the Connection in VS Code

1. **Install the Snowflake VS Code Extension**
   Search for **“Snowflake”** in the VS Code Extensions Marketplace and install the official extension.

2. **Create or edit the Snowflake configuration file**
   VS Code uses a file called `connections.toml`.
   Place it in your Snowflake extension settings directory or workspace root.

   Example `connections.toml`:

   ```toml
   [OSCGTUT-OK42230]
   account = "OSCGTUT-OK42230"
   user = "SVC_TEST_ETL"
   authenticator = "SNOWFLAKE_JWT"
   private_key_path = "/path/to/your/private_key.pem"
   role = "ROLE_TEST_ETL"
   warehouse = "COMPUTE_WH"
   database = "TESTING_DB"
   schema = "INTEGRAL_TEST"
   ```

3. **Select the connection in VS Code**

   * Open the Snowflake sidebar (Snowflake icon on the left).
   * Click **Connections → Add / Select Connection**.
   * Choose `OSCGTUT-OK42230`.

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
