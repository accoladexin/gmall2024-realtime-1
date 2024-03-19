package com.acco.gmall.realtime.common.base;

import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * ClassName: BaseAPP
 * Description: None
 * Package: com.acco.gmall.realtime.common.base
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-22 19:47
 */
public abstract  class BaseAPP {
    public void start(int port,int Parallelism,String ckAndGroupID,String topicName) {
        // TODO 1.启动UI和构建环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(Parallelism);

        // TODO 2.设置检查点和状态后端
        env.enableCheckpointing(5000L);// 设置检查点
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// 精准一次
        // 设置检查点存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2024" + ckAndGroupID);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 设置检查点的并发数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);//设置检查点的最小间隔
        env.getCheckpointConfig().setCheckpointTimeout(10000);//超时时间
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // TODO 3.读取数据
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupID, topicName);
        DataStreamSource<String> kafkaSource = env.fromSource(
               source, // 来源
                WatermarkStrategy.noWatermarks(), // 水位线
                "kafka_source"); // source 名字 好像可以随便取
        handle(env,kafkaSource);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream);
}



