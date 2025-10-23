use role accountadmin;

create share hexa_share_object;
show shares;

show databases;
grant usage on database project_db to share hexa_share_object;
grant usage on schema PROJECT_DB.CDC_SCH to share hexa_share_object;
grant select on table PROJECT_DB.CDC_SCH.MEMBERS_PROD to share hexa_share_object;

create managed account deloitte_account
admin_name = deloitteadmin
admin_password = 'ShinyNewP@ssword123'
type = reader;          ---------------reader account

----"https://tyhngza-deloitte_account.snowflakecomputing.com"

alter share hexa_share_object add account = tyhngza.deloitte_account;