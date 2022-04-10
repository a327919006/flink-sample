package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.io.File;

/**
 * connect合流，只能合并两个流，返回结果类型允许不同
 *
 * @author Chen Nan
 */
public class Test8_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file1 = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataStream<String> fileData = env.readTextFile(file1.getAbsolutePath());

        File file2 = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file2.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .connect(fileData)
                .map(new CoMapFunction<SensorData, String, Object>() {
                    @Override
                    public Object map1(SensorData value) throws Exception {
                        return new Tuple3<>(value.getId(), value.getName(), value.getValue());
                    }

                    @Override
                    public Object map2(String value) throws Exception {
                        return new Tuple2<>(value, value.length());
                    }
                })
                .print();

        env.execute();
    }
}
