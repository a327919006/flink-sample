-- flinkSql
CREATE TABLE kafka_test_starrocks_user
(
    name  STRING,
    score INTEGER
) WITH (
      'connector' = 'kafka',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true',
      'properties.bootstrap.servers' = '192.168.5.132:39573',
      'properties.group.id' = 'flink-starrocks-consumer',
      'topic' = 'test_starrocks'
      );

CREATE TABLE sr_test_starrocks_user
(
    name  STRING,
    score INTEGER
) WITH (
      'connector' = 'starrocks',
      'jdbc-url' = 'jdbc:mysql://192.168.5.131:19030',
      'load-url' = '192.168.5.131:18030',
      'username' = 'test',
      'password' = '123456',
      'database-name' = 'example_db',
      'table-name' = 'student_score',
      'sink.properties.format' = 'json',
      'sink.properties.strip_outer_array' = 'true',
      'sink.buffer-flush.interval-ms' = '1000'
      );

INSERT INTO sr_test_starrocks_user
SELECT *
FROM kafka_test_starrocks_user;


