package com.cn.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.time.Duration;

/**
 * 从文件流式读取数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/filesystem/#file-source
 *
 * @author Chen Nan
 */
public class SourceTest2_FileStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TextLineFormat format = new TextLineFormat("UTF-8");

        File file = new File("sample-base\\src\\main\\resources");
        FileSource<String> source = FileSource.forRecordStreamFormat(format, new Path(file.getAbsolutePath()))
                // 设置为流处理模式（文件读取完成后不会退出），未设置则为批处理模式（文件读取完成后退出）
                // 持续监听目录下是否有新增文件（修改原有文件内容不会被监听）
                // 监听到后就会读取到新文件内容
                // 此处可设置监听周期
                .monitorContinuously(Duration.ofSeconds(5))
                .build();


        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test-file-source")
                .print();

        env.execute();
    }
}
