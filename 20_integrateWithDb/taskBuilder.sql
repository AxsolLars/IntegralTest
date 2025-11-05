
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