SET
table.sql-dialect=hive;
CREATE TABLE test.user_location_partition_info_temporal
(
    `cert_type`   string,
    `cert_nbr`    string,
    `lat`         string,
    `lng`         string,
    `lat_night`   string,
    `lng_night`   string,
    `work_day`    date,
    `destination` string
) PARTITIONED BY (`partstart` STRING) TBLPROPERTIES (
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '30 s',
  'streaming-source.partition-order' = 'create-time'
);

partstart = 20220217 )
SELECT cert_type, cert_nbr, lat, lng, lat_night, lng_night, work_day, destination
FROM test.user_location_source_info;

INSERT overwrite TABLE test.user_location_partition_info_temporal PARTITION (
partstart = 20220218 )
SELECT cert_type, cert_nbr, lat, lng, lat_night, lng_night, work_day, destination
FROM test.user_location_source_info2;

/*------------------------------------------------------------------------*/

SET
table.sql-dialect=hive;
CREATE TABLE test.user_location_partition_info_temporal
(
    `cert_type`   string,
    `cert_nbr`    string,
    `lat`         string,
    `lng`         string,
    `lat_night`   string,
    `lng_night`   string,
    `work_day`    date,
    `destination` string
) PARTITIONED BY (`partstart` STRING) TBLPROPERTIES (
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'all'
);