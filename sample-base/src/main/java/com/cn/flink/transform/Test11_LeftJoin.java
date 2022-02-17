package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataResult;
import com.cn.flink.domain.SensorSubData;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * coGroup合流，实现InnerJoin或LeftJoin或RightJoin
 *
 * @author Chen Nan
 */
public class Test11_LeftJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> streamOne = env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));

        DataStream<SensorSubData> streamTwo = env.socketTextStream("127.0.0.1", 8888)
                .map((MapFunction<String, SensorSubData>) value -> {
                    String[] split = value.split(",");
                    SensorSubData sensorData = new SensorSubData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorSubData.class));

        streamOne.coGroup(streamTwo)
                .where((KeySelector<SensorData, Long>) SensorData::getId)
                .equalTo((KeySelector<SensorSubData, Long>) SensorSubData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((CoGroupFunction<SensorData, SensorSubData, SensorDataResult>) (first, second, out) -> {
                    List<SensorData> firstList = new ArrayList<>();
                    List<SensorSubData> secondList = new ArrayList<>();
                    first.forEach(firstList::add);
                    second.forEach(secondList::add);

                    for (SensorData sensorData : firstList) {
                        // 定义flag，表示left流中的key在right流中是否存在
                        boolean hasSubData = false;
                        for (SensorSubData sensorSubData : secondList) {
                            Double result = sensorData.getValue() + sensorSubData.getValue();
                            Long timestamp = sensorData.getTimestamp();
                            if (sensorSubData.getTimestamp() > timestamp) {
                                timestamp = sensorSubData.getTimestamp();
                            }
                            out.collect(new SensorDataResult(sensorData.getId(), sensorData.getName(), result, timestamp));
                            hasSubData = true;
                        }
                        if (!hasSubData) {
                            // 如果right中不存在，则value设为0
                            out.collect(new SensorDataResult(sensorData.getId(), sensorData.getName(), 0D, sensorData.getTimestamp()));
                        }
                    }


                }, TypeInformation.of(SensorDataResult.class))
                .print();

        env.execute();
    }
}
