package com.cn.flink.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Chen Nan
 */
public class SqlCdc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 需指定主键，否则报错，或设置scan.incremental.snapshot.enabled为false
        tableEnv.executeSql("CREATE TABLE student (" +
                " id INT," +
                " name STRING," +
                " sex INT," +
                " PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'scan.startup.mode' = 'initial'," + // 或latest-offset
                " 'hostname' = '127.0.0.1'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'chennan'," +
                " 'database-name' = 'cntest'," +
                " 'table-name' = 'student'" +
                ")");

//        tableEnv.executeSql("select * from student").print();
        Table table = tableEnv.sqlQuery("select * from student");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute();
    }
}
