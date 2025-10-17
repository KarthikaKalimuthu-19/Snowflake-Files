-----------------------One to many table streaming----------------------

use role developer_role;
use warehouse dev_warehouse;
create database parking_db;
use database parking_db;

------------------------------------------------------------------------

create or replace table parking_NEW -- NJ_parking --NY_parking
(
Summons_Number Number ,
Plate_ID Varchar ,
Registration_State Varchar ,
Plate_Type Varchar ,
Issue_Date DATE ,
Violation_Code Number ,
Vehicle_Body_Type Varchar ,
Vehicle_Make Varchar ,
Issuing_Agency Varchar ,
Street_Code1 Number ,
Street_Code2 Number ,
Street_Code3 Number ,
Vehicle_Expiration_Date Number ,
Violation_Location Varchar ,
Violation_Precinct Number ,
Issuer_Precinct Number ,
Issuer_Code Number ,
Issuer_Command Varchar ,
Issuer_Squad Varchar ,
Violation_Time Varchar ,
Time_First_Observed Varchar ,
Violation_County Varchar ,
Violation_In_Front_Of_Or_Opposite Varchar ,
House_Number Varchar ,
Street_Name Varchar ,
Intersecting_Street Varchar ,
Date_First_Observed Number ,
Law_Section Number ,
Sub_Division Varchar ,
Violation_Legal_Code Varchar ,
Days_Parking_In_Effect Varchar ,
From_Hours_In_Effect Varchar ,
To_Hours_In_Effect Varchar ,
Vehicle_Color Varchar ,
Unregistered_Vehicle Varchar ,
Vehicle_Year Number ,
Meter_Number Varchar ,
Feet_From_Curb Number ,
Violation_Post_Code Varchar ,
Violation_Description Varchar ,
No_Standing_or_Stopping_Violation Varchar ,
Hydrant_Violation Varchar ,
Double_Parking_Violation Varchar ,
Latitude Varchar,
Longitude Varchar,
Community_Board Varchar,
Community_Council Varchar,
Census_Tract Varchar,
BIN Varchar,
BBL Varchar,
NTA Varchar
);

create table NY_PARKING_TABLE like PARKING_NEW;
create table NJ_PARKING_TABLE like PARKING_NEW;

-----------------------------Creating stream------------------------------------

create stream NY_STREAM on table PARKING_NEW;
create stream NJ_STREAM on table PARKING_NEW;

-----------------------------creating internal stage----------------------------

create stage parking_internal_stage;

--put file//D:\myparkingdata\* @parking_internal_stage;   ----ran this command in cmd 

list @parking_internal_stage;

--------------------------------------------------------------------------------

create task parking_data_load_task
warehouse = dev_warehouse
schedule = '1 minute'
as
copy into parking_new
from @parking_internal_stage
ON_ERROR = 'CONTINUE'
file_format = (type = 'csv', error_on_column_count_mismatch = false);

-----------------------------verifying both streams-------------------------------

select * from NY_STREAM;  --54,043
select * from NJ_STREAM;  --54,043

----------------------------------------------------------------------------------
--- new york data

create task ny_data_load_task
warehouse = dev_warehouse
schedule = '1 minute'
when
    system$stream_has_data('NY_STREAM')
as
INSERT INTO NY_PARKING_TABLE
(
Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,
Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,
Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,
Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,
Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,
Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,
No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,
Community_Council, Census_Tract, BIN, BBL, NTA
)
SELECT
Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,
Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,
Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,
Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,
Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,
Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,
No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,
Community_Council, Census_Tract, BIN, BBL, NTA
FROM NY_STREAM WHERE Registration_State='NY' AND metadata$action = 'INSERT';
 ---37568 datas

--------------------------------------------------------
---New jersey data

create task nj_data_load_task
warehouse = dev_warehouse
schedule = '1 minute'
when
    system$stream_has_data('NJ_STREAM')
as
 INSERT INTO NJ_PARKING_TABLE
(
Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,
Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,
Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,
Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,
Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,
Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,
No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,
Community_Council, Census_Tract, BIN, BBL, NTA
)
SELECT
Summons_Number, Plate_ID, Registration_State, Plate_Type, Issue_Date, Violation_Code, Vehicle_Body_Type, Vehicle_Make,
Issuing_Agency, Street_Code1, Street_Code2, Street_Code3, Vehicle_Expiration_Date, Violation_Location, Violation_Precinct,
Issuer_Precinct, Issuer_Code, Issuer_Command, Issuer_Squad, Violation_Time, Time_First_Observed, Violation_County,
Violation_In_Front_Of_Or_Opposite, House_Number, Street_Name, Intersecting_Street, Date_First_Observed, Law_Section,
Sub_Division, Violation_Legal_Code, Days_Parking_In_Effect, From_Hours_In_Effect, To_Hours_In_Effect, Vehicle_Color,
Unregistered_Vehicle, Vehicle_Year, Meter_Number, Feet_From_Curb, Violation_Post_Code, Violation_Description,
No_Standing_or_Stopping_Violation, Hydrant_Violation, Double_Parking_Violation, Latitude, Longitude, Community_Board,
Community_Council, Census_Tract, BIN, BBL, NTA
FROM NJ_STREAM WHERE Registration_State='NJ' AND metadata$action = 'INSERT';
---5641 datas

select * from ny_parking_table limit 1000;
select * from nj_parking_table limit 1000;

select count(*) from parking_new;

--------------------------Project Automation--------------------------------

show stages;

---removing extra files for making the project to work automatically

remove @parking_internal_stage/xab.gz;
remove @parking_internal_stage/xac.gz;

list @parking_internal_stage;

----------------------------------------------------------------------------

show tasks;

alter task PARKING_DATA_LOAD_TASK resume;
alter task NY_DATA_LOAD_TASK resume;
alter task Nj_DATA_LOAD_TASK resume;

----------------------------------checking status---------------------------------

--------------------------------files:  --xaa   --xab    --xac
select count(*) from parking_new;       --54043 --106432 --153788
select count(*) from ny_parking_table;  --37568 --75472  --108443
select count(*) from nj_parking_table;  --5641  --12161  --18403

----------------------------------getting history----------------------------------

select * from table(information_schema.copy_history(table_name=>'parking_new', 
                    start_time=> dateadd(day, -1, current_timestamp())));

----------------------------------stoping tasks-------------------------------------

alter task PARKING_DATA_LOAD_TASK suspend;
alter task NY_DATA_LOAD_TASK suspend;
alter task Nj_DATA_LOAD_TASK suspend;

show tasks;
-----------------------set time travel for 7 days for data protection--------------------

show tables;
alter table ny_parking_table set data_retention_time_in_days = 7;
alter table nj_parking_table set data_retention_time_in_days = 7;