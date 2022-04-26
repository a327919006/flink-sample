package com.cn.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义函数：表聚合函数
 * 实现Top2
 *
 * @author Chen Nan
 */
public class Test15_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File inputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String inputFilePath = inputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + inputFilePath + "'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(inputTableSql);

        // 注册全局函数MyDataFunction，并指定实现类
        tableEnv.createTemporarySystemFunction("Top2", MyTopTwoFunction.class);


        // Table result = tableEnv.sqlQuery(
        //         "SELECT id, window_end AS endT, MAX(`value`) AS mv " +
        //                 "FROM TABLE( " +
        //                 "  TUMBLE( TABLE inputTable, DESCRIPTOR(`ts`), INTERVAL '5' SECOND)" +
        //                 ") " +
        //                 "GROUP BY id, window_start, window_end "
        // );

        // 使用自定义函数进行查询
        Table table = tableEnv.sqlQuery("SELECT id, `value` " +
                "FROM inputTable "
        );

        Table result = table.groupBy($("id"))
                .flatAggregate(call("Top2", $("value")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"));

        tableEnv.toChangelogStream(result)
                .print();
        env.execute();
    }

    public static class TopTwo {
        public Double topOneValue = Double.MIN_VALUE;
        public Double topTwoValue = Double.MIN_VALUE;
    }

    public static class MyTopTwoFunction extends
            TableAggregateFunction<Tuple2<Double, Integer>, TopTwo> {

        @Override
        public TopTwo createAccumulator() {
            return new TopTwo();
        }

        public void accumulate(TopTwo accumulator, Double currValue) {
            if (currValue > accumulator.topOneValue) {
                accumulator.topOneValue = currValue;
            } else if (currValue > accumulator.topTwoValue) {
                accumulator.topTwoValue = currValue;
            }
        }

        public void emitValue(TopTwo accumulator, Collector<Tuple2<Double, Integer>> collector) {
            if (accumulator.topOneValue != Double.MIN_VALUE) {
                collector.collect(Tuple2.of(accumulator.topOneValue, 1));
            }
            if (accumulator.topTwoValue != Double.MIN_VALUE) {
                collector.collect(Tuple2.of(accumulator.topTwoValue, 2));
            }
        }
    }
}
