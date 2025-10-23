use role accountadmin;
use warehouse dev_warehouse;
use schema SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;

select store.s_store_id, item.i_item_id, sum(ss_sales_price) ss_sales_price
from store_sales
    ,item
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_item_sk = item.i_item_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and store.s_store_name = 'ese'
group by store.s_store_id, item.i_item_id;

------for small warehouse --2m 59s

alter warehouse dev_warehouse
set
warehouse_size = 'MEDIUM';

------for medium warehouse --1m 8s

------disable cache

alter session set use_cached_result = false;

alter warehouse dev_warehouse
set
warehouse_size = 'large';

------for large warehouse --39s

alter warehouse dev_warehouse
set
warehouse_size = 'x-large';

------for x-large warehouse --20s