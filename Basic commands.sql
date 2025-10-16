select current_role();
select current_user();
show roles;
drop role snowflake_learning_role;
show roles;

--------------------------------------
use role orgadmin;
select current_role();

use role securityadmin;
use role sysadmin;

---------------------------------------
use role accountadmin;

---------------------------------------
use role accountadmin;
show databases;
use database SNOWFLAKE_SAMPLE_DATA;
show schemas;
use schema tpch_sf1;
show tables;
select * from lineitem limit 100;

select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CALL_CENTER limit 10;
select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CATALOG_RETURNS limit 100;