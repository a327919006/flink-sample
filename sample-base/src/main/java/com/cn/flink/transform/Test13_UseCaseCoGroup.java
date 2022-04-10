package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataDetail;
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
 * CoGroupFunction合流，允许降窗口周期内的数据在窗口结束时统一处理
 * 实现窗口周期内左流中的数据与右流中的合并
 * 应用场景：物联网，设备每隔1分钟内会上报一次统计数据和明细数据，分开在两个kafka主题
 * 此时希望将统计数据和明细数据合并成一个字符串串保存（在一个窗口周期内统计数据只有一条，明细数据有多条）
 * 举例：
 * 左流1条数据：1,sensor1,30,1640000000000
 * 右流2条数据：1,sensor1,10,1640000000000和1,sensor1,20,1640000000000
 * join结果输出1条：主1,sensor1,30,1640000000000，明细：1,sensor1,10,1640000000000和1,sensor1,20,1640000000000
 *
 * @author Chen Nan
 */
public class Test13_UseCaseCoGroup {
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

        streamLeft.coGroup(streamRight)
                .where((KeySelector<SensorData, Long>) SensorData::getId)
                .equalTo((KeySelector<SensorSubData, Long>) SensorSubData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((CoGroupFunction<SensorData, SensorSubData, SensorDataDetail>) (first, second, out) -> {
                    List<SensorData> firstList = new ArrayList<>();
                    List<SensorSubData> secondList = new ArrayList<>();
                    first.forEach(firstList::add);
                    second.forEach(secondList::add);

                    if (firstList.size() > 0) {
                        SensorDataDetail detail = new SensorDataDetail();
                        SensorData sensorData = firstList.get(0);
                        detail.setSensorData(sensorData);
                        detail.setSensorSubDataList(secondList);
                        out.collect(detail);
                    }

                }, TypeInformation.of(SensorDataDetail.class))
                .print();

        env.execute();
    }
}
