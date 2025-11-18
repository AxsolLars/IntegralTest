
CREATE OR REPLACE TASK powermeter_integrate_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
CALL powermeter_integrate();

CREATE OR REPLACE TASK power_production_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
CALL power_production_integrate();

CREATE OR REPLACE TASK daily_consumption_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * Europe/Berlin'
AS 
CALL sum_history(CURRENT_DATE() - 1);