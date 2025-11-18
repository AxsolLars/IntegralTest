
ALTER TASK powermeter_integrate_task RESUME;
ALTER TASK powermeter_integrate_task  SUSPEND;
ALTER TASK power_production_task RESUME;
ALTER TASK power_production_task  SUSPEND;
ALTER TASK daily_consumption_task RESUME;
ALTER TASK daily_consumption_task SUSPEND;
EXECUTE TASK powermeter_integrate_task;