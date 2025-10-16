use role developer_role;
use warehouse dev_warehouse;
use database project_db;
create schema cdc_sch;
use schema cdc_sch;

create or replace table members (
  id number(8),
  name varchar(255),
  fee number(3)
);

create or replace stream member_check on table members;

select * from members;
select * from member_check;

create or replace table members_prod (
  id number(8),
  name varchar(255),
  fee number(3)
);

select * from members_prod;

insert into members (id, name, fee)
values
(1, 'Joe', 0);

select * from members;
select * from member_check;

insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';  --after adding data into mem_pro then stream become empty

select * from members_prod;

-----------------------------------------------------

insert into members (id,name,fee)
values
(2,'Jane',0),
(3,'George',0);

select * from member_check;

insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';

select * from members_prod;

-----------------------------------------------------------

insert into members (id,name,fee)
values
(4,'Betty',0),
(5,'Sally',0);

select * from member_check;

insert into members_prod(id,name,fee) 
select id, name, fee from member_check 
where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE';

select * from members_prod;

---------------------------Deleting record-------------------------------

delete from members 
where name = 'Sally';

select * from member_check;

delete from members_prod where name in (
  select distinct name from member_check
  where METADATA$ACTION = 'DELETE' and METADATA$ISUPDATE = 'FALSE'
);

select * from members_prod;

------------------------------Updating the record-------------------------------

update members 
set fee = 100
where id = 1;

select * from member_check;

