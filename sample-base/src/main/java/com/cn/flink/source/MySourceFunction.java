package com.cn.flink.source;

import com.cn.flink.domain.SensorData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义Souce
 * 如果是实现SourceFunction接口，则并行度只能设为1，否则会抛异常
 * 如果是实现ParallelSourceFunction接口，则可设置并行度
 * 如果是继承RichParallelSourceFunction，则可以使用open和close方法，用于初始化或关闭资源
 *
 * @author Chen Nan
 */
public class MySourceFunction extends RichParallelSourceFunction<SensorData> {
    // 自定义停止标识
    private boolean stop = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        int num = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("sourceFunction open, taskNum=" + num);
    }

    @Override
    public void close() throws Exception {
        int num = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("sourceFunction close, taskNum=" + num);
    }

    @Override
    public void run(SourceContext<SensorData> ctx) throws Exception {
        SensorData sensorData = new SensorData();
        while (!stop) {
            long id = RandomUtils.nextLong(1, 5);
            double value = RandomUtils.nextInt(1, 100);
            sensorData.setId(id);
            sensorData.setName("sensor" + id);
            sensorData.setValue(value);
            sensorData.setTimestamp(System.currentTimeMillis());
            ctx.collect(sensorData);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
