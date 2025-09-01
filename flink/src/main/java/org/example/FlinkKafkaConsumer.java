package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkKafkaConsumer {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3); // 设置并行度为1，便于观察结果
//        env.getCheckpointConfig().setCheckpointInterval(5000); // 每5秒做一次checkpoint
//          env.enableCheckpointing(5000); // 每5秒做一次checkpoint
        // 配置Kafka源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.231.131:9092")
                .setTopics("test-topic")
                .setGroupId("flink-word-count-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 创建数据流并处理
        DataStream<Tuple2<String, Integer>> wordCounts = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                )
                .flatMap(new Tokenizer()) // 分词
                .keyBy(tuple -> tuple.f0) // 按单词分组
                .sum(1); // 统计词频

        // 输出结果（实际应用中可能写入另一个Kafka主题或数据库）
        wordCounts.print();

        env.execute("Flink Kafka Word Count");
    }

    // 分词函数
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
