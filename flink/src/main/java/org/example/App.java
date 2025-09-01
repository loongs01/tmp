package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class App {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接 socket 获取输入数据
        DataStream<String> text = env.socketTextStream("localhost", 9999).setParallelism(1);

        // 处理数据
        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new LineSplitter())
                .keyBy(value -> value.f0)
                .sum(1);

        // 输出结果
        counts.print();

        // 执行程序
        env.execute("Socket Window WordCount");
    }

    // 自定义 FlatMapFunction
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\s")) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
//public class Apps {}