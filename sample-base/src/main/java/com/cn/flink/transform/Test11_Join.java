package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataResult;
import com.cn.flink.domain.SensorSubData;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * JoinFunction合流，实现innerJoin
 * 两个流在窗口时间内都有数据，则会输出结果，与SQL中的innerJoin相同
 * 举例1:
 * 左流1条数据：1,sensor1,10,1640000000000
 * 右流1条数据：1,sensor1,20,1640000000000
 * join结果输出1条：1,sensor1,30,1640000000000
 * <p>
 * 举例2：
 * 左流1条数据：1,sensor1,10,1640000000000
 * 右流2条数据：1,sensor1,20,1640000000000和1,sensor1,30,1640000000000
 * join结果输出2条：1,sensor1,30,1640000000000和1,sensor1,40,1640000000000
 * 结果为左流分别与右流join的结果，与MySQL中的join类似
 *
 * @author Chen Nan
 */
public class Test11_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> streamLeft = env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));

        DataStream<SensorSubData> streamRight = env.socketTextStream("127.0.0.1", 8888)
                .map((MapFunction<String, SensorSubData>) value -> {
                    String[] split = value.split(",");
                    SensorSubData sensorData = new SensorSubData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorSubData.class));

        streamLeft.join(streamRight)
                .where((KeySelector<SensorData, Long>) SensorData::getId)
                .equalTo((KeySelector<SensorSubData, Long>) SensorSubData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((JoinFunction<SensorData, SensorSubData, SensorDataResult>) (first, second) -> {
                    double result = first.getValue() + second.getValue();
                    Long timestamp = first.getTimestamp();
                    if (second.getTimestamp() > timestamp) {
                        timestamp = second.getTimestamp();
                    }
                    return new SensorDataResult(first.getId(), first.getName(), result, timestamp);
                })
                .print();

        env.execute();
    }
}
