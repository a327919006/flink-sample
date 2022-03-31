package com.cn.flink.processfunction;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;

/**
 * 分流，旧版中是split方法实现，从flink1.13起弃用
 * 新版使用侧输出流实现分流效果。
 * 使用案例：分流，根据value值大小，按value是否大于20分成两个流
 *
 * @author Chen Nan
 */
public class Test3_UseCaseSplit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<SensorData> lowTag = new OutputTag<SensorData>("low") {
        };

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        SingleOutputStreamOperator<SensorData> highStream = env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .process(new ProcessFunction<SensorData, SensorData>() {
                    private Double high = 30D;

                    @Override
                    public void processElement(SensorData value, Context ctx, Collector<SensorData> out) throws Exception {
                        if (value.getValue() > high) {
                            out.collect(value);
                        } else {
                            ctx.output(lowTag, value);
                        }
                    }
                });

        DataStream<SensorData> lowStream = highStream.getSideOutput(lowTag);
        highStream.print("high");
        lowStream.print("low");

        env.execute();
    }

}
