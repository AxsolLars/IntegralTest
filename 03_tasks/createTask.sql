USE ROLE role_test_etl;
CREATE OR REPLACE TASK mirror_random_values_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
  INSERT INTO random_values_mirror (id, timestamp, value)
  SELECT id, timestamp, value
  FROM random_values_stream;
