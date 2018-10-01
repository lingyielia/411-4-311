-- create table for storing historical data
create table events (
  agency                 varchar(255)     not null,
  closed_date            timestamp        not null,
  complaint_type         varchar(255)     not null,
  created_date           timestamp        not null,
  latitude               double precision not null,
  longitude              double precision not null,
  open_data_channel_type varchar(255)     not null
);

-- table store most recent records only
create table eventsNew (
  agency                 varchar(255)     not null,
  closed_date            timestamp        not null,
  complaint_type         varchar(255)     not null,
  created_date           timestamp        not null,
  latitude               double precision not null,
  longitude              double precision not null,
  open_data_channel_type varchar(255)     not null
);

-- check the table being successfully built
select distinct(tablename) from PG_TABLE_DEF where schemaname = 'public';

-- temperarily change the concurrency level to 2
set wlm_query_slot_count to 2;

-- copy data from s3
copy events from 's3://nyc311forinsight/records_'
credentials 'aws_access_key_id=<>;aws_secret_access_key=<>'
csv
timeformat 'YYYY-MM-DDTHH:MI:SS';
