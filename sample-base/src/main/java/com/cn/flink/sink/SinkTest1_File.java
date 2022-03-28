package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * sink-将数据写入Kafka
 *
 * @author Chen Nan
 */
public class SinkTest1_File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSink<String> sink = FileSink
                // 自定输出文件路径及编码
                .forRowFormat(new Path("./output"), new SimpleStringEncoder<String>("UTF-8"))
                // 指定文件滚动策略，类似日志每隔一段时间生成新文件、或文件达到指定大小生成新文件
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 每隔15分钟生成新文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 当空闲5分钟没有新数据则生成新文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // 当文件大小达到1G则生成新文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

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
                .map(SensorData::toString)
                .sinkTo(sink);

        env.execute();
    }
}
