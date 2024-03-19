import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * ClassName: Test01
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-22 19:01
 */
public class Test01 {
    public static void main(String[] args) {
        // TODO 1.构建环境
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // TODO 2.设置检查点和状态后端
        env.enableCheckpointing(5000L);// 设置检查点
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 精准一次
        // 设置检查点存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2024"+"/acco");
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1) ; // 设置检查点的并发数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);//设置检查点的最小间隔
        env.getCheckpointConfig().setCheckpointTimeout(10000);//超时时间
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // TODO 3.读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")
                        .setTopics("topic_db")
                        .setStartingOffsets(OffsetsInitializer.earliest())//从那个地方开始读
                        .setGroupId("test01")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );
        kafkaSource.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
