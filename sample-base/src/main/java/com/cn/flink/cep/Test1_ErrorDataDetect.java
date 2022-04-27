package com.cn.flink.cep;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * CRP入门：模式匹配
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/libs/cep/
 * CEP已经考虑到数据乱序问题，只需定义好Watermark即可
 *
 * @author Chen Nan
 */
public class Test1_ErrorDataDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data_cep.txt");
        DataStream<SensorData> dataStream = env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                ).keyBy(SensorData::getId);

        // 1. 定义Pattern模式匹配规则，定义值突然飙高回落的事件
        Pattern<SensorData, SensorData> pattern = Pattern.<SensorData>begin("firstData")
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        return sensorData.getValue() < 50;
                    }
                })
                .next("secondData")
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        return sensorData.getValue() > 100;
                    }
                })
                // 允许指定事件出现次数，默认为1次
                // 1、oneOrMore()1次或多次,
                // 2、greedy()尽可能多，将连续多次匹配成一个List输出到patternStream，而不是每两个就输出一次
                // .oneOrMore().greedy()
                // 3、times()指定出现次数
                // .times(2)
                // 4、optional()当前事件可不出现
                // .optional()
                .next("thirdData")
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        return sensorData.getValue() < 50;
                    }
                });

        // 2. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<SensorData> patternStream = CEP.pattern(dataStream, pattern);

        // 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream.select((PatternSelectFunction<SensorData, String>) map -> {
            SensorData firstData = map.get("firstData").get(0);
            SensorData secondData = map.get("secondData").get(0);
            SensorData thirdData = map.get("thirdData").get(0);
            int count = map.get("secondData").size();

            String warm = "id=" + firstData.getId() + "数值从" + firstData.getValue() +
                    "飙高到" + secondData.getValue() + "飙高次数" + count +
                    "又回落到" + thirdData.getValue();

            return warm;
        }).print();

        env.execute();
    }
}
