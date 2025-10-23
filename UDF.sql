use role developer_role;
use database project_db;
use warehouse dev_warehouse;
create schema udf_sch;
 
-- Simple UDF ----

CREATE or replace FUNCTION sum_values(a number,b number)
  RETURNS number
  LANGUAGE SQL 
  AS
  $$
    SELECT a+b as res
  $$;

select sum_values(2,3);

--------------------------------------------------

create or replace table LINEITEM cluster by (L_SHIPDATE) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."LINEITEM";
create or replace table ORDERS as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."ORDERS";
create or replace table SUPPLIER as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."SUPPLIER";

select sales_qty_by_supplier('1997-03-19',58750);
select sales_qty_by_supplier('1997-03-19',5852);
select sales_qty_by_supplier('1997-03-19',35113) as total_qty_shipped;
 -----------------------------------------------------------------------

CREATE or replace FUNCTION sales_qty_by_supplier(ship_date date,supplier_key integer)
  RETURNS NUMERIC(11,2)
  AS
  $$
    SELECT SUM(l_quantity) as total_quantity_shipped
        FROM PROJECT_DB.UDF_SCH.LINEITEM
        where L_SHIPDATE =ship_date and l_suppkey=supplier_key
  $$;