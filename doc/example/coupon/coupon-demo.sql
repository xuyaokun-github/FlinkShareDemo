CREATE TABLE `test.user_location_source`(
  `cert_type` string,
  `cert_nbr` string,
  `lat` string,
  `lng` string,
  `work_day` date,
  `destination` string)
COMMENT 'This is a coupon test table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LINES  TERMINATED BY '\n';

CREATE TABLE `test.user_location_partition`(
  `cert_type` string,
  `cert_nbr` string,
  `lat` string,
  `lng` string,
  `work_day` date,
  `destination` string)
COMMENT 'This is a coupon test table'
PARTITIONED by (`partstart` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LINES  TERMINATED BY '\n';

/* 分区导数 */
INSERT overwrite TABLE test.user_location_partition PARTITION (
partstart = 20220207 )
SELECT cert_type, cert_nbr, lat, lng, work_day, destination
FROM test.user_location_source;

INSERT overwrite TABLE test.user_location_partition PARTITION (
partstart = 20220210 )
SELECT cert_type, cert_nbr, lat, lng, work_day, destination
FROM test.user_location_source;

/* 清数据*/
truncate table user_location_source;
truncate table user_location_partition;
