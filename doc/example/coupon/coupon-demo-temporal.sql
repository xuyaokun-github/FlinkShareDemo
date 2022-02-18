SET table.sql-dialect=hive;
CREATE TABLE test.user_location_partition_info_temporal (
  `cert_type` string,
  `cert_nbr` string,
  `lat` string,
  `lng` string,
  `lat_night` string,
  `lng_night` string,
  `work_day` date,
  `destination` string
) PARTITIONED BY (`partstart` STRING) TBLPROPERTIES (
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '10 s',
  'streaming-source.partition-order' = 'create-time'
);


