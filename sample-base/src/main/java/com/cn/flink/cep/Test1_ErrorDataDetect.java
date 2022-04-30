package com.cn.flink.cep;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * CRP入门：模式匹配
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/libs/cep/
 * CEP已经考虑到数据乱序问题，只需定义好Watermark即可
 * <p>
 * 应用示例：检测数据飙高回落事件
 * 检测用户连续登陆失败多次
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
        Pattern<SensorData, SensorData> pattern = Pattern.
                // 1.4、匹配后跳过策略:因为存在oneOrMore和followedBy，会造成连续的事件匹配多次，输出多次，如a1->a2->a3->b
                // 就会输出 a1b a1a2b a1a2a3b a2b a2a3b a3b六次
                // 1.4.1、默认noSkip()不跳过，所有匹配的事件都输出
                // 1.4.2、skipToNext跳至下个事件（a1 a2 a3 b），（a2 a3 b），（a3 b）效果同greedy
                // 1.4.3、skipPastLastEvent（a1 a2 a3 b） 最精简
                // 1.4.4、skipToFirst只保留以a1开始的事件（a1 a2 a3 b），（a1 a2 b），（a1 b）
                // 1.4.5、skipToLast只保留含a3的事件（a1 a2 a3 b），（a3 b）
                        <SensorData>begin("firstData")
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        return sensorData.getValue() < 50;
                    }
                })
                // 1.1、定义模式序列：begin初始模式
                // 1.1.1、next严格近邻：下一个事件必须是指定事件 A->B
                // 1.1.2、followedBy宽松近邻：中间允许有其他事件 A->xxx->B
                // 1.1.3、followedByAny：非确定性宽松近邻，上个条件只需一次，后续事件每一次出现都会触发匹配一次 A->XXX->B->XXX->B
                // 1.1.4、notNext：下个事件必须不是指定事件  A->!B
                // 1.1.5、notFollowedBy：后续事件不能出现指定事件，不能作为结尾，必须再跟着一个条件，用法：AC事件间不能由B  A->!B->C
                .next("secondData")
                // .followedBy("secondData")

                // 1.2、指定条件：如value>100
                // 1.2.1、如果还有其他条件可以使用.where()表示and，.or()表示或
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        // 此处可根据业务需要，获取到之前已经匹配到的事件，包括自己secondData已匹配的事件
                        // Iterable<SensorData> firstData = context.getEventsForPattern("firstData");
                        return sensorData.getValue() > 100;
                    }
                })
                // .or(xxxxxxxxxx)
                // .where(xxxxxxxx)

                // 1.3、指定量词：允许指定事件出现次数，默认为1次
                // 1.3.1、oneOrMore()1次或多次，默认为宽松近邻
                // 1.3.2、greedy()尽可能多，将连续多次匹配成一个List输出到patternStream，而不是每两个就输出一次
                // 1.3.3、times()指定出现次数，默认为宽松近邻
                // 1.3.4、consecutive()严格近邻，否则默认为宽松近邻
                // 1.3.5、allowCombinations（）非确定性宽松近邻
                // 1.3.6、optional()当前事件可不出现
                // .oneOrMore().greedy()
                .times(2).consecutive()
                .next("thirdData")
                .where(new IterativeCondition<SensorData>() {
                    @Override
                    public boolean filter(SensorData sensorData, Context<SensorData> context) throws Exception {
                        return sensorData.getValue() < 50;
                    }
                })
                // 1.4、时间限制：上述模式必须在指定时间范围内的数据上匹配
                .within(Time.seconds(10));

        // 2. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<SensorData> patternStream = CEP.pattern(dataStream, pattern);

        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        OutputTag<String> lateTag = new OutputTag<String>("late") {
        };
        // 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        SingleOutputStreamOperator<String> result = patternStream
                .sideOutputLateData()
                .process(new MyPatternProcessFunction(timeoutTag));
        DataStream<String> timeoutResult = result.getSideOutput(timeoutTag);

        result.print("result");
        timeoutResult.print("timeout");

        env.execute();
    }

    public static class MyPatternProcessFunction extends PatternProcessFunction<SensorData, String>
            implements TimedOutPartialMatchHandler<SensorData> {
        private OutputTag<String> timeoutTag;

        public MyPatternProcessFunction(OutputTag<String> timeoutTag) {
            this.timeoutTag = timeoutTag;
        }

        /**
         * 处理正常匹配事件
         */
        @Override
        public void processMatch(Map<String, List<SensorData>> map, Context context, Collector<String> collector) throws Exception {
            SensorData firstData = map.get("firstData").get(0);
            SensorData secondData = map.get("secondData").get(0);
            SensorData thirdData = map.get("thirdData").get(0);
            int count = map.get("secondData").size();

            String warm = "id=" + firstData.getId() + "数值从" + firstData.getValue() +
                    "飙高到" + secondData.getValue() + "飙高次数" + count +
                    "又回落到" + thirdData.getValue();

            collector.collect(warm);
        }

        /**
         * 处理超时事件：已匹配到部分事件，未全部匹配前已超时（配置了within才会超时）
         */
        @Override
        public void processTimedOutMatch(Map<String, List<SensorData>> match, Context ctx) throws Exception {
            SensorData firstData = match.get("firstData").get(0);
            SensorData secondData = null;
            int count = 0;
            if (match.containsKey("secondData")) {
                secondData = match.get("secondData").get(0);
                count = match.get("secondData").size();
            }

            if (count > 0) {
                String warm = "id=" + firstData.getId() + "数值从" + firstData.getValue() +
                        "飙高到" + secondData.getValue() + "飙高次数" + count +
                        "超时未回落";

                ctx.output(timeoutTag, warm);
            }
        }
    }
}
