use role developer_role;
use database project_db;
create schema tasks_sch;
use schema tasks_sch;

-------------------------------------------------------------

CREATE TABLE EMPLOYEES(EMPLOYEE_NAME VARCHAR DEFAULT 'Instructor',
                       LOAD_TIME DATE);

---scheduling using minutes

CREATE OR REPLACE TASK EMPLOYEES_TASK
  WAREHOUSE = DEV_WAREHOUSE
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO EMPLOYEES(LOAD_TIME) VALUES(CURRENT_TIMESTAMP);

select * from EMPLOYEES;

show tasks;

---from accountadmin we need permission to access task

alter task EMPLOYEES_TASK resume;

select * from table(information_schema.task_history())
order by scheduled_time;

drop table employees;

undrop table employees;

select * from employees;

----------------------------------------------------------------

list @PROJECT_DB.PUBLIC.MY_AZURE_STAGE;

create table task_lineitems
like
PROJECT_DB.PUBLIC.MY_LINEITEMS;

select * from task_lineitems;

---changing us time into indian time

select current_timestamp();
alter session set timezone = 'UTC';
select current_timestamp();

---creating task and scheduling using cron expression

create task data_load_lineitem
warehouse = dev_warehouse
schedule = 'USING CRON 46 04 * * * UTC'
as
copy into task_lineitems
from @PROJECT_DB.PUBLIC.MY_AZURE_STAGE;

---to start a task

alter task data_load_lineitem resume;

---for seeing history

select * from table(information_schema.task_history())
order by scheduled_time;

---suspend task

alter task data_load_lineitem suspend;