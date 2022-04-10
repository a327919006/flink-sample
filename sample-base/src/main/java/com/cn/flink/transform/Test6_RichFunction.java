package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.io.File;

/**
 * RichFunction富函数，有open、close方法以及可以获取到运行上下文
 *
 * @author Chen Nan
 */
public class Test6_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .map(new RichMapFunction<String, Tuple2<Integer, SensorData>>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 一般用于初始化状态或数据库连接等，根据并行度，每个分区都会调用一次此方法
                        System.out.println("init database success");
                    }

                    @Override
                    public Tuple2<Integer, SensorData> map(String value) throws Exception {
                        // 从上下文中获取当前线程号
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        String[] split = value.split(",");
                        SensorData sensorData = new SensorData();
                        sensorData.setId(Long.parseLong(split[0]));
                        sensorData.setName(split[1]);
                        sensorData.setValue(Double.parseDouble(split[2]));
                        sensorData.setTimestamp(Long.parseLong(split[3]));
                        return new Tuple2<>(indexOfThisSubtask, sensorData);
                    }

                    @Override
                    public void close() throws Exception {
                        // 一般用于清空状态或断开数据库连接，每个分区都会调用一次此方法
                        System.out.println("close database success");

                    }
                })
                .print();

        env.execute();
    }
}
