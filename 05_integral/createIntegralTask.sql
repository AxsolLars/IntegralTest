CREATE OR REPLACE TASK integrate_stream_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
CALL integrate_stream();
