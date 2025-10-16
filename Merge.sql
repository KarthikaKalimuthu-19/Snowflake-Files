use role developer_role;
use warehouse dev_warehouse;
use database project_db;
create schema merge_sch;
use schema merge_sch;

create or replace table members (
  id number(8),
  name varchar(255),
  fee number(3)
);

create or replace stream member_check on table members;

create or replace table members_prod (
  id number(8),
  name varchar(255),
  fee number(3)
);

insert into members (id, name, fee)
values
(1, 'Joe', 0);

MERGE INTO members_prod a USING member_check b ON a.ID = b.ID
 WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' 
   THEN DELETE
 WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' 
   THEN UPDATE SET a.FEE = b. FEE, a.NAME = b.NAME
 WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
   THEN INSERT (ID, NAME, FEE) VALUES (b.ID, b.NAME, b.FEE);

select * from members_prod;

-----update

update members 
set fee = 100
where id = 1;

MERGE INTO members_prod a USING member_check b ON a.ID = b.ID
 WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' 
   THEN DELETE
 WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' 
   THEN UPDATE SET a.FEE = b. FEE, a.NAME = b.NAME
 WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
   THEN INSERT (ID, NAME, FEE) VALUES (b.ID, b.NAME, b.FEE);

select * from members_prod;

----delete

delete from members 
where name = 'Joe';

MERGE INTO members_prod a USING member_check b ON a.ID = b.ID
 WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE' 
   THEN DELETE
 WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE' 
   THEN UPDATE SET a.FEE = b. FEE, a.NAME = b.NAME
 WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
   THEN INSERT (ID, NAME, FEE) VALUES (b.ID, b.NAME, b.FEE);

select * from members_prod;