use role developer_role;
create database parking_dynamic_db;

-------dynamic tables are child table and always needs parent table-------

create stage my_d_stage;

create table parking_new
like
PARKING_DB.PUBLIC.PARKING_NEW;

select * from parking_new;

--------------------ny dynamic table---------------------

create dynamic table ny_parking_d_table
target_lag = '1 minute'
warehouse = dev_warehouse
as
select * from parking_new
where registration_state = 'NY';

--------------------nj dynamic table----------------------

create dynamic table nj_parking_d_table
target_lag = '1 minute'
warehouse = dev_warehouse
as
select * from parking_new
where registration_state = 'NJ';

select count(*) from parking_new;
select count(*) from ny_parking_d_table;
select count(*) from nj_parking_d_table;

---task for parking_new file

create task parking_dynamic_data_load_task
warehouse = dev_warehouse
schedule = '1 minute'
as
copy into parking_new
from @my_d_stage
ON_ERROR = 'CONTINUE'
file_format = (type = 'csv', error_on_column_count_mismatch = false);

---starting tasks

alter task parking_dynamic_data_load_task resume;

---verifying datas

select count(*) from parking_new;  --54043  --106432
select count(*) from ny_parking_d_table;  --37568  --75472
select count(*) from nj_parking_d_table;  --5641  --12161

show dynamic tables;

---stopping tasks

alter dynamic table NJ_PARKING_D_TABLE suspend;
alter dynamic table NY_PARKING_D_TABLE suspend;
alter task parking_dynamic_data_load_task suspend;

show tasks;