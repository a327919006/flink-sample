package com.cn.flink.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

/**
 * kafka读写数据
 * 测试此用例需先在kafka上创建主题sensorInput
 * 然后写入数据1,sensor1,10.1,1640000000000
 * 16进制为312c73656e736f72312c31302e312c31363430303030303030303030
 * <p>
 * 然后结果可查看主题sensorOutput
 *
 * @author Chen Nan
 */
public class Test4_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema inputSchema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.DECIMAL(10, 2))
                .column("timestamp", DataTypes.BIGINT())
                .build();
        Schema outputSchema = Schema.newBuilder()
                .column("deviceId", DataTypes.BIGINT())
                .column("deviceName", DataTypes.STRING())
                .column("data", DataTypes.DECIMAL(10, 2))
                .column("createTime", DataTypes.BIGINT())
                .build();

        TableDescriptor inputDesc = TableDescriptor.forConnector("kafka")
                .schema(inputSchema)
                .option("properties.bootstrap.servers", "127.0.0.1:9092")
                .option("topic", "sensorInput")
                .option("properties.group.id", "flink-consumer")
                .option("scan.startup.mode", "earliest-offset")
                .format("csv")
                .build();
        TableDescriptor outputDesc = TableDescriptor.forConnector("kafka")
                .schema(outputSchema)
                .option("properties.bootstrap.servers", "127.0.0.1:9092")
                .option("topic", "sensorOutput")
                .format("json")
                .build();
        tableEnv.createTemporaryTable("inputTable", inputDesc);
        tableEnv.createTemporaryTable("outputTable", outputDesc);

        Table inputTable = tableEnv.from("inputTable");
        Table filterTable = inputTable.select($("id"),
                $("name"), $("value"), $("timestamp"))
                .where($("id").isEqual(1));

        filterTable.executeInsert("outputTable");
    }
}
