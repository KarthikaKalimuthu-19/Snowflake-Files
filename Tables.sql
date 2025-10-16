use role developer_role;
use database project_db;
create schema tables_sch;
use schema tables_sch;
use warehouse dev_warehouse;
----------------------

create or replace stage my_table_az_stage
url = 'azure://hexakarthika.blob.core.windows.net/mycontainer1'
credentials = (AZURE_SAS_TOKEN = 'sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-10-30T12:12:13Z&st=2025-10-08T03:57:13Z&spr=https&sig=GjQne3jWmMaiUDM7iOYJvdobNY0ZOGi9UJGTVdYgWmw%3D');

list @my_table_az_stage;

------------------------External tables---------------------------

create external table ext_lineitems_t
  with location = @my_table_az_stage
  auto_refresh = false
  file_format = (type = csv);

select * from ext_lineitems_t limit 10;
--------------------------

select value:"c1",value:"c2" from ext_lineitems_t limit 10;
-----------------------------------

---lets re-create this table with proper column names for our data analytics--------
CREATE or replace EXTERNAL TABLE ext_lineitems_t
(
        ORDERKEY number AS (value:c1::number),
        PARTKEY number AS (value:c2::number),
        SUPPKEY number AS (value:c3::number),
        LINENUMBER number AS (value:c4::number),
        QTY number AS (value:c5::number),
        PRICE number AS (value:c6::number)
)
  with location=@my_table_az_stage
  auto_refresh = false
  file_format = (type = csv);

select * from ext_lineitems_t limit 10;
  
select ORDERKEY, QTY from ext_lineitems_t limit 10;

select ORDERKEY, QTY from ext_lineitems_t
where QTY < 20;

-----------------------------------
select count(*) from ext_lineitems_t;

alter external table ext_lineitems_t refresh;

------------------------------Temp table/Internal table----------------------------

list @-;

create temporary table lineitems_tmp
like
PROJECT_DB.PUBLIC.MY_LINEITEMS;

copy into lineitems_tmp
from @~;

select * from lineitems_tmp limit 100;

select count(*) from lineitems_tmp;
select L_ORDERKEY, L_QUANTITY from lineitems_tmp where L_EXTENDEDPRICE < 25000;