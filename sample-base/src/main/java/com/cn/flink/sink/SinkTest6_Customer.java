package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义sink-将数据写入mysql
 * CREATE TABLE `cntest`.`sensor_data`  (
 * `id` bigint(0) NOT NULL,
 * `name` varchar(255),
 * `value` decimal(10, 2),
 * `time` bigint(0),
 * PRIMARY KEY (`id`)
 * )
 *
 * @author Chen Nan
 */
public class SinkTest6_Customer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                .addSink(new RichSinkFunction<SensorData>() {
                    Connection conn = null;
                    PreparedStatement upsertStmt = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        String url = "jdbc:mysql://127.0.0.1:3306/cntest";
                        String user = "root";
                        String password = "chennan";
                        conn = DriverManager.getConnection(url, user, password);
                        // 创建预编译器，有占位符，可传入参数
                        String sql = "INSERT INTO sensor_data(id,name,value,time) VALUES" +
                                " (?,?,?,?)" +
                                " ON DUPLICATE KEY UPDATE" +
                                " value=VALUES(value)," +
                                " time=VALUES(time)";
                        upsertStmt = conn.prepareStatement(sql);
                    }

                    @Override
                    public void invoke(SensorData value, Context context) throws Exception {
                        upsertStmt.setLong(1, value.getId());
                        upsertStmt.setString(2, value.getName());
                        upsertStmt.setDouble(3, value.getValue());
                        upsertStmt.setLong(4, value.getTimestamp());
                        upsertStmt.execute();
                    }

                    @Override
                    public void close() throws Exception {
                        upsertStmt.close();
                        conn.close();
                    }
                });

        env.execute();
    }
}
