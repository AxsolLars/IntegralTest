
CALL power_production_integrate();
CALL powermeter_integrate();

CALL sum_history(CURRENT_DATE() - 1);