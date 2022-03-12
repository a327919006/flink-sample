package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用sql从Kafka一个topic读取数据到MySQL
 * 支持使用聚合函数，将流式聚合结果upsert至mysql
 *
 * @author Chen Nan
 */
public class Test5_UseCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'sensorInput'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'flinkTest'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'csv'" +
                ")";
        String outputTableSql = "CREATE TABLE outputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://127.0.0.1:3306/cntest?useSSL=false'," +
                "  'table-name' = 'sensor_output'," +
                "  'username' = 'root'," +
                "  'password' = 'chennan'" +
                ")";

        String insertSql = "INSERT INTO outputTable " +
                " SELECT `id`, `name`, MAX( `value`) AS `value`, max(`time`)" +
                " FROM inputTable" +
                " GROUP BY `id`, name";

        tableEnv.executeSql(inputTableSql);
        tableEnv.executeSql(outputTableSql);
        tableEnv.executeSql(insertSql);
    }
}
