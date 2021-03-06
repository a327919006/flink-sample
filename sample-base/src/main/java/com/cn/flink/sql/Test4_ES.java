package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用sql从Mysql一个表读取数据到ES中
 * 设置主键后会自动实现upsert效果，否则是Append模式
 *
 * @author Chen Nan
 */
public class Test4_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://127.0.0.1:3306/cntest?useSSL=false'," +
                "  'table-name' = 'sensor_data'," +
                "  'username' = 'root'," +
                "  'password' = 'chennan'" +
                ")";
        String outputTableSql = "CREATE TABLE outputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-7'," +
                "  'hosts' = 'http://localhost:9200'," +
                "  'index' = 'sensor_output'" +
                ")";

        String insertSql = "INSERT INTO outputTable " +
                "SELECT id, name, `value`, `time` " +
                "FROM inputTable " +
                "WHERE id = 1 ";

        tableEnv.executeSql(inputTableSql);
        tableEnv.executeSql(outputTableSql);
        tableEnv.executeSql(insertSql);
    }
}
