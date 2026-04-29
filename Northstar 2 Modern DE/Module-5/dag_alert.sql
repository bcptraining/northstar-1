/*
    dag_alert.sql
    
    This script manages the Tasty Bytes Hamburg sales DAG and sets up
    an alert to monitor DAG completion.

    DAG Structure:
      1. PROCESS_ORDERS_HEADER_SPROC (root task, scheduled)
      2. SEND_LAST_SEVEN_DAYS_REPORT (child task, runs after root)

    Alert:
      DAG_COMPLETION_ALERT checks every 5 minutes for completed DAG runs
      (success or failure) since its last evaluation. When a run is detected,
      it emails bcpurkistraining@gmail.com with the overall status and a
      per-task breakdown. Uses the email_notification_int integration
      defined in dag_email_integration.sql.

    Usage:
      Run statements top-to-bottom to start the DAG and enable the alert.
      Run the suspend statements at the bottom to stop everything.
*/

-- Create child task that sends the weekly Hamburg sales report email
-- Runs after the root task completes successfully
CREATE OR REPLACE TASK tasty_bytes.raw_pos.send_last_seven_days_report
WAREHOUSE = 'COMPUTE_WH'
AFTER tasty_bytes.raw_pos.process_orders_header_sproc
AS
CALL tasty_bytes.raw_pos.last_seven_days_report();

-- Resume tasks (child first, then root)
ALTER TASK tasty_bytes.raw_pos.send_last_seven_days_report RESUME;
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

-- Manually trigger the DAG
EXECUTE TASK tasty_bytes.raw_pos.process_orders_header_sproc;

-- Suspend tasks (root first, then child)
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;
ALTER TASK tasty_bytes.raw_pos.send_last_seven_days_report SUSPEND;

-- Alert: polls TASK_HISTORY every 5 min and emails DAG status (success or failure)
CREATE OR REPLACE ALERT tasty_bytes.raw_pos.dag_completion_alert
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM TABLE(TASTY_BYTES.INFORMATION_SCHEMA.TASK_HISTORY(
      SCHEDULED_TIME_RANGE_START => GREATEST(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(), DATEADD('day', -6, CURRENT_TIMESTAMP())),
      RESULT_LIMIT => 100
    ))
    WHERE NAME = 'PROCESS_ORDERS_HEADER_SPROC'
      AND STATE IN ('SUCCEEDED', 'FAILED')
  ))
  THEN
    BEGIN
      LET task_results VARCHAR;
      LET dag_status VARCHAR;

      SELECT LISTAGG(NAME || ': ' || STATE, '\n') WITHIN GROUP (ORDER BY SCHEDULED_TIME)
        INTO :task_results
      FROM TABLE(TASTY_BYTES.INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => GREATEST(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(), DATEADD('day', -6, CURRENT_TIMESTAMP())),
        RESULT_LIMIT => 100
      ))
      WHERE NAME IN ('PROCESS_ORDERS_HEADER_SPROC', 'SEND_LAST_SEVEN_DAYS_REPORT');

      SELECT CASE WHEN CONTAINS(:task_results, 'FAILED') THEN 'FAILED' ELSE 'SUCCEEDED' END
        INTO :dag_status;

      CALL SYSTEM$SEND_EMAIL(
        'email_notification_int',
        'bcpurkistraining@gmail.com',
        'DAG Status: ' || :dag_status,
        'DAG run completed with status: ' || :dag_status || '\n\nTask Results:\n' || :task_results
      );
    END;

-- Activate the alert
ALTER ALERT tasty_bytes.raw_pos.dag_completion_alert RESUME;


