package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * reduce应用案例：
 * 统计所有sensor中，数据量最大的sensor
 * 结果：数据量都是4条，取最先到4条的sensor1
 *
 * @author Chen Nan
 */
public class Test5_UseCaseReduse {
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
                .map((MapFunction<SensorData, Tuple2<String, Integer>>) value -> Tuple2.of(value.getName(), 1),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }))
                .keyBy(data -> data.f0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (oldValue, newValue) -> Tuple2.of(oldValue.f0, oldValue.f1 + newValue.f1))
                // 这里的key可以是任意字符串，目的是将上个步骤得到的结果，全部传入同一个slot，计算最大值
                // 上个步骤处理后的结果只是少量数据，已经可以使用单个slot计算
                .keyBy(data -> "key")
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (oldValue, newValue) -> oldValue.f1 >= newValue.f1 ? oldValue : newValue)
                .print();

        env.execute();
    }
}
