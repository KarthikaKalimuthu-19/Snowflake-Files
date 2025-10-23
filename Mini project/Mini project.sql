------------------------------------------KARTHIKA KALIMUTHU MINI PROJECT------------------------------------------------

use role developer_role;
use warehouse dev_warehouse;
create database miniproject_db;
use database miniproject_db;

---------creating table

create or replace table Australiaa (
Customer_ID number,
Customer_Name varchar,
Customer_Email varchar,
Customer_City varchar,
Customer_State varchar,
Customer_DOB date
);

create table nsw_t1 like Australiaa;
create table cb_t1 like Australiaa;
create table ql_t1 like Australiaa;

------creating streams

create stream nsw_stm on table Australiaa;
create stream cb_stm on table Australiaa;
create stream ql_stm on table Australiaa;

------creating external stage

create or replace stage my_mini_project1
url = 'azure://hexakarthika.blob.core.windows.net/mycontainer2'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:12:13Z&st=2025-10-08T03:57:13Z&spr=https&sig=GjQne3jWmMaiUDM7iOYJvdobNY0ZOGi9UJGTVdYgWmw%3D');

list @my_mini_project1;

------creating file format

create file format my_project
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1;

------copying data from stage

copy into Australiaa
from @my_mini_project1
file_format = my_project;  

select * from AUSTRALIAA;  ---436

------checking the streams

select * from nsw_stm;  ---100 datas  ---112 datas  --103  --121
select * from cb_stm;  ---100 datas  ---112 datas  --103  --121
select * from ql_stm;  ---100 datas  ---112 datas  --103  --121

------inserting data into NSW table

insert into nsw_t1
(
Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
)
select Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
from nsw_stm where Customer_State='New South Wales' AND metadata$action = 'INSERT';    ---17 datas  ---20  -17  --24

------inserting data into cb table

insert into cb_t1
(
Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
)
select Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
from cb_stm where Customer_State='Victoria' AND metadata$action = 'INSERT';   ---13 datas  ---15 datas  --16  --17

------inserting data into ql table

insert into ql_t1
(
Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
)
select Customer_ID, Customer_Name, Customer_Email, Customer_City, Customer_State, Customer_DOB
from ql_stm where Customer_State='Queensland' AND metadata$action = 'INSERT';    ---14 datas  ---14 datas  --14  --17

select * from nsw_t1;  --17  --37  --54  --78
select * from cb_t1; --13  --28  --44  --61
select * from ql_t1;  --14  --28  --42  --59