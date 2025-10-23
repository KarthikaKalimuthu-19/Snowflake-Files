/*creating vendor role and warehouse in admin account

use role securityadmin;
create role vendor_role;
show roles;

--create a user for assuming the developer role

create user v_user1
password = 'doe'
default_role = vendor_role
must_change_password= false;

grant role vendor_role to user v_user1;

---creating warehouse

use role sysadmin;

create or replace warehouse vendor_warehouse
warehouse_size = 'SMALL'
auto_suspend = 60;

grant usage on warehouse vendor_warehouse to role vendor_role;

*/

use role developer_role;
use database parking_db;

grant usage on database parking_db to role vendor_role;
grant usage on schema parking_db.public to role vendor_role;
grant select on table PARKING_DB.PUBLIC.NJ_PARKING_TABLE to role vendor_role;
grant select on table PARKING_DB.PUBLIC.NY_PARKING_TABLE to role vendor_role;
grant select on view PARKING_DB.PUBLIC.CUSTOMER_VIEW to role vendor_role;

create masking policy sensitive_numbers
as (val number) returns number ->
  case 
    when current_role() in ('developer_role', 'data_analyst_role') then val
    else '999999999999'
  end;

create masking policy sensitive_strings
as (val string()) returns string ->
  case 
    when current_role() in ('developer_role', 'data_analyst_role') then val
    else '************'
  end;

--------------------applying masking policies to respective columns in any table-----------------------

alter table PARKING_DB.PUBLIC.NJ_PARKING_TABLE
modify column summons_number
set masking policy sensitive_numbers;

alter table PARKING_DB.PUBLIC.NJ_PARKING_TABLE
modify column plate_id
set masking policy sensitive_strings;

-----------------------creating view------------------------

create view customer_view
as
select * from PROJECT_DB.PUBLIC.MY_CUSTOMER;

grant select on view PARKING_DB.PUBLIC.CUSTOMER_VIEW to role vendor_role;

create masking policy partial_strings 
as (val STRING) returns STRING ->
case
  when current_role() in ('DEVELOPER_ROLE', 'DATA_ANALYST_ROLE') then val
  when current_role() in ('VENDOR_ROLE') then regexp_replace(val,'.+\@','*****@')
  else '********'
end;

ALTER VIEW PARKING_DB.PUBLIC.CUSTOMER_VIEW 
MODIFY COLUMN CUSTOMER_EMAIL 
SET MASKING POLICY partial_strings;
 