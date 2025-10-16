-----------------view table history data and restoring data---------------------

use role developer_role;
use warehouse dev_warehouse;
use database project_db;
create schema time_sch;
use schema time_sch;

--------------------------------

list @PROJECT_DB.PUBLIC.MY_AZURE_STAGE2;

create table customer_t
like 
PROJECT_DB.PUBLIC.MY_CUSTOMER;

show tables in schema time_sch;

alter table customer_t 
set
data_retention_time_in_days = 7;

--------------------------------------

select current_timestamp();

alter session set timezone = 'UTC';
select current_timestamp();

---------------------------------------

--1st file

copy into customer_t
from @PROJECT_DB.PUBLIC.MY_AZURE_STAGE2/generated_customer_data.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);  -----100 data

--2nd file

copy into customer_t
from @PROJECT_DB.PUBLIC.MY_AZURE_STAGE2/generated_customer_data2.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);  -----112 data --total= 212 data

--3rd file

copy into customer_t
from @PROJECT_DB.PUBLIC.MY_AZURE_STAGE2/generated_customer_data3.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);  -----103 data --total= 315 data

--4th update of any state

select count(*) from customer_t;
select * from customer_t;
select * from customer_t where customer_state = 'New South Wales';  -----54

update customer_t
set customer_state = 'New Delhi'
where customer_state = 'New South Wales';

select * from customer_t;
select * from customer_t where customer_state = 'New Delhi';

-------------------------------Time back in time using time stamp-------------------------------------

select * from customer_t at(timestamp => '2025-10-15 04:56:11.642 +0000'::timestamp);  -----04:54:11 by changing time we can see the history

-------------------------------Time back in time using query id------------------------------important

--q1 = 01bfb826-0001-6698-000c-36ae00031eaa --file1
--q2 = 01bfb828-0001-669b-000c-36ae00038832 --file2
--q3 = 01bfb82a-0001-669b-000c-36ae00038852 --file3
--q3 = 01bfb82e-0001-669b-000c-36ae000388fa --update query

select * from customer_t at(statement => '01bfb82e-0001-669b-000c-36ae000388fa'); --after query executed
select * from customer_t before(statement => '01bfb82e-0001-669b-000c-36ae000388fa'); --before query executed

----------------------------Recover data - original new south wales 315-------------------------------------

create table my_recoverd_t
clone
customer_t before(statement => '01bfb82e-0001-669b-000c-36ae000388fa');

select * fromC

delete from customer_t
where customer_state = 'New Delhi';

insert into customer_t
select * from my_recoverd_t where customer_state = 'New South Wales';

drop table my_recoverd_t;

----------------------------------Undrop-------------------------------------

use schema public;
select * from emp;

drop table emp;
undrop table emp;  -------dropping table

drop schema int_schema;
undrop schema int_schema;

------------------------------------------------------------------------------

create database drop_db;

create table t2
like
PROJECT_DB.PUBLIC.MY_CUSTOMER;

drop database drop_db; 
undrop database drop_db;  -------dropping database

show parameters in database drop_db;

alter database drop_db 
set
data_retention_time_in_days = 7;
-------------------------------------------------------------------------------
