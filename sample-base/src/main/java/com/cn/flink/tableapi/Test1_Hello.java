package com.cn.flink.tableapi;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

/**
 * map一对一转换，一个输入只能通过return返回一个输出
 *
 * @author Chen Nan
 */
public class Test1_Hello {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        DataStream<SensorData> dataStream = env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(dataStream);
        Table resultTable = table
                .select($("id"), $("name"), $("value"), $("timestamp"))
                .where($("id").isEqual(1));

        tableEnv.createTemporaryView("sensor", table);
        String sql = "select id, name, `value` from sensor where id=1";
        Table resultSqlTable = tableEnv.sqlQuery(sql);


        tableEnv.toDataStream(resultTable, SensorData.class).print("resultTable");
        tableEnv.toDataStream(resultSqlTable, Row.class).print("resultSqlTable");
        env.execute();
    }
}
