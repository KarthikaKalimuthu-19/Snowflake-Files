use database project_db;
create schema int_schema;
use schema int_schema;

create stage my_int_stage;
show stages;

create table lineitems
like
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

copy into lineitems 
from @my_int_stage;

select count(*) from lineitems;

------------------------------------------
create stage my_int_stage1;

create table customer
like
my_customer;

--put file://D:\customer\*.* @my_int_stage1;

list @my_int_stage1;

create file format my_custom_csv1
type = csv
FIELD_DELIMITER = '|'
SKIP_HEADER = 1;

copy into customer
from @my_int_stage1
file_format = my_custom_csv1;

select count(*) from customer;

-------------table stage of lineitems---------------
list @%lineitems;

--put file://D:\customer\*.* @%lineitems;

copy into lineitems
from @%lineitems;

select count(*) from lineitems;

--------------User's local/personal internal stage--------
list @-;

--put file://D:\lineitems\*.* @-;

copy into lineitems
from @-;

select count(*) from lineitems;
-------------