

-- This SQL file is for the Hands On Lab Guide for the 30-day free Snowflake trial account
-- The numbers below correspond to the sections of the Lab Guide in which SQL is to be run in a Snowflake worksheet
-- Modules 1 - 3 of the Lab Guide have no SQL to be run


/* *********************************************************************************** */
/* *** MODULE 4  ********************************************************************* */
/* *********************************************************************************** */

create or replace table trips  
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

-- List stage data
list @citibike_trips;

--create file format

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

--verify file format is created

show file formats in database citibike;


/* *********************************************************************************** */
/* *** MODULE 5  ********************************************************************* */
/* *********************************************************************************** */


copy into trips from @citibike_trips
file_format=CSV;


-- Truncate

truncate table trips;

--verify table is clear
select * from trips limit 10;


-- Reload with bigger warehouse
--change warehouse size from small to large (up to 6x!)
alter warehouse compute_wh set warehouse_size='large';


copy into trips from @citibike_trips
file_format=CSV;

/* *********************************************************************************** */
/* *** MODULE 6  ********************************************************************* */
/* *********************************************************************************** */


select * from trips limit 20;


select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;


select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;


select
    dayname(starttime) as "day of week",
    count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- Clone a table!
create table trips_dev clone trips;



/* *********************************************************************************** */
/* *** MODULE 7  ********************************************************************* */
/* *********************************************************************************** */

create database weather;


use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;


create table json_weather_data (v variant);


create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';


list @nyc_weather;

copy into json_weather_data 
from @nyc_weather 
file_format = (type=json);


select * from json_weather_data limit 10;


create view json_weather_data_view as
select
  v:time::timestamp as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

-- 

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' 
limit 20;

-- Join your data with external data

select weather as conditions
    ,count(*) as num_trips
from citibike.public.trips 
left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


/* *********************************************************************************** */
/* *** MODULE 7.1********************************************************************* */
/* *********************************************************************************** */

--Set context
use role sysadmin;
create or replace warehouse pipe_wh with warehouse_size = 'medium' warehouse_type = 'standard' auto_suspend = 120 auto_resume = true;
use warehouse pipe_wh;
create database if not exists citibike;
use schema citibike.public;

--create trips table to load data from pipe
create or replace table trips_stream 
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  end_station_id integer,
  bikeid integer,
  usertype string,
  birth_year integer,
  gender integer,
  program_id integer);

--create weather table to load data from pipe
create or replace table json_weather_stream (v variant);

--create stage for trips
create or replace stage pipe_data_trips url = 's3://snowflake-workshop-lab/snowpipe/trips/' file_format=(type=csv);

--create stage for weather
create or replace stage pipe_data_weather url = 's3://snowflake-workshop-lab/snowpipe/weather/' file_format=(type=json);

list @pipe_data_trips;
list @pipe_data_weather;

/* *********************************************************************************** */
/* *** MODULE 7.2 ******************************************************************** */
/* *********************************************************************************** */

-- create the trips pipe using SNS topic
create or replace pipe trips_pipe auto_ingest=true 
aws_sns_topic='arn:aws:sns:us-east-1:484577546576:snowpipe_sns_lab' 
as copy into trips_stream from @pipe_data_trips/;

-- create the weather pipe using SNS topic
create or replace pipe weather_pipe auto_ingest=true 
aws_sns_topic='arn:aws:sns:us-east-1:484577546576:snowpipe_sns_lab' 
as copy into json_weather_stream from @pipe_data_weather/;

show pipes;

--check trips pipe status
select system$pipe_status('trips_pipe');

--check weather pipe status
select system$pipe_status('weather_pipe');

/* *********************************************************************************** */
/* *** MODULE 7.3 ******************************************************************** */
/* *********************************************************************************** */

-- show the files that have been processed
select *
from table(information_schema.copy_history(table_name=>'TRIPS_STREAM', start_time=>dateadd('hour', -1, CURRENT_TIMESTAMP())));

-- show the files that have been processed
select *
from table(information_schema.copy_history(table_name=>'JSON_WEATHER_STREAM', start_time=>dateadd('hour', -1, CURRENT_TIMESTAMP())));

-- show the data landing in the table
select count(*) from trips_stream;

select count(*) from json_weather_stream;

select * from trips_stream limit 5;

select * from json_weather_stream limit 5;

-- and we can unwrap complex structures such as arrays via FLATTEN
-- to compare the most common weather in different cities
select value:main::string as conditions
  ,sum(iff(v:city.name::string='New York',1,0)) as nyc_freq
  ,sum(iff(v:city.name::string='Seattle',1,0)) as seattle_freq
  ,sum(iff(v:city.name::string='San Francisco',1,0)) as san_fran_freq
  ,sum(iff(v:city.name::string='Miami',1,0)) as miami_freq
  ,sum(iff(v:city.name::string='Washington, D. C.',1,0)) as wash_freq
  from json_weather_stream w,
  lateral flatten (input => w.v:weather) wf
  where v:city.name in ('New York','Seattle','San Francisco','Miami','Washington, D. C.')
    --and year(t) = 2019
  group by 1;


/* *********************************************************************************** */
/* *** MODULE 8  ********************************************************************* */
/* *********************************************************************************** */

-- Oops

drop table json_weather_data;

-- 

Select * from json_weather_data limit 10;

-- 

undrop table json_weather_data;


use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;


update trips set start_station_name = 'oops';


select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


-- Get the queryid from the previous query
set query_id = 
(select query_id from 
table(information_schema.query_history_by_session (result_limit=>5)) 
where query_text like 'update%' order by start_time limit 1);

create or replace table trips as
(select * from trips before (statement => $query_id));
        
-- 

select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


/* *********************************************************************************** */
/* *** MODULE 9  ********************************************************************* */
/* *********************************************************************************** */

-- 

use role accountadmin; 

--  (NOTE - enter your unique user name into the second row below)

create role junior_dba;
grant role junior_dba to user YOUR_USER_NAME_GOES HERE;

-- 

use role junior_dba;

-- 

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

-- 

use role junior_dba;

-- OPTIONAL reset script to remove all objects created in the lab

use role accountadmin;
use warehouse compute_wh;
use database weather;
use schema public;



drop share if exists trips_share;
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;




