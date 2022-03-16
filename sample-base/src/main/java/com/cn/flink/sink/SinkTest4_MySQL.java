package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;

/**
 * sink-将数据写入MySQL
 *
 * @author Chen Nan
 */
public class SinkTest4_MySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String sql = "INSERT INTO sensor_data(id,name,value,time) VALUES" +
                " (?,?,?,?)" +
                " ON DUPLICATE KEY UPDATE" +
                " value=VALUES(value)," +
                " time=VALUES(time)";

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://127.0.0.1:3306/cntest?useSSL=false")
                .withUsername("root")
                .withPassword("chennan")
                .build();
        SinkFunction<SensorData> sink = JdbcSink.sink(sql, (JdbcStatementBuilder<SensorData>) (ps, sensorData) -> {
            ps.setLong(1, sensorData.getId());
            ps.setString(2, sensorData.getName());
            ps.setDouble(3, sensorData.getValue());
            ps.setLong(4, sensorData.getTimestamp());
        }, connectionOptions);


        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .addSink(sink);

        env.execute();
    }
}
