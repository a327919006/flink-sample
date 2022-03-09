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
 * mysql
 *
 * @author Chen Nan
 */
public class Test5_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        String filePath = file.getAbsolutePath();

        Schema inputSchema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.DECIMAL(10, 2))
                .column("timestamp", DataTypes.BIGINT())
                .build();
        Schema outputSchema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.DECIMAL(10, 2))
                .column("time", DataTypes.BIGINT())
                .build();

        TableDescriptor inputDesc = TableDescriptor.forConnector("filesystem")
                .schema(inputSchema)
                .option("path", filePath)
                .format("csv")
                .build();
        TableDescriptor outputDesc = TableDescriptor.forConnector("jdbc")
                .schema(outputSchema)
                .option("url", "jdbc:mysql://127.0.0.1:3306/cntest?useSSL=false")
                .option("username", "root")
                .option("password", "chennan")
                .option("table-name", "sensor_data")
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
