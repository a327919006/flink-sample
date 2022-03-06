package com.cn.flink.tableapi;

import com.cn.flink.domain.SensorData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

/**
 * map一对一转换，一个输入只能通过return返回一个输出
 *
 * @author Chen Nan
 */
public class Test2_CommonAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        String filePath = file.getAbsolutePath();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // flink1.14前有区分planner和blink版planner，1.14后删除了旧版planner，无需指定planner
                // .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.DECIMAL(10, 2))
                .column("timestamp", DataTypes.BIGINT())
                .build();

        TableDescriptor filesystem = TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", filePath)
                .format("csv")
                .build();
        tableEnv.createTemporaryTable("inputTable", filesystem);

        Table inputTable = tableEnv.from("inputTable");
        // 输出表结构
        inputTable.printSchema();
        tableEnv.toDataStream(inputTable, SensorData.class).print();

        env.execute();
    }
}
