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
 * 结果输出到文件
 *
 * @author Chen Nan
 */
public class Test3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File intputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String intputFilePath = intputFile.getAbsolutePath();
        File outputFile = new File("sample-base\\src\\main\\resources\\result");
        String outputFilePath = outputFile.getAbsolutePath();

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

        TableDescriptor inputDesc = TableDescriptor.forConnector("filesystem")
                .schema(inputSchema)
                .option("path", intputFilePath)
                .format("csv")
                .build();
        TableDescriptor outputDesc = TableDescriptor.forConnector("filesystem")
                .schema(outputSchema)
                .option("path", outputFilePath)
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
