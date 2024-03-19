package com.acco.gmall.realtime.common.util;

import com.acco.gmall.realtime.common.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * ClassName: FlinkSinkUtil
 * Description: None
 * Package: com.acco.gmall.realtime.common.util
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-02 20:12
 */
public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchemaBuilder<String>()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("acco"+topic + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15*60*1000+"")
                .build();
    }
    public static KafkaSink<JSONObject> getKafkaSinkWithTopicName(){
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                        String topicName = element.getString("sink_table");
                        element.remove("sink_table");
                        return new ProducerRecord<>(topicName, Bytes.toBytes(element.toJSONString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("acco"+"base_db" + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15*60*1000+"")
                .build();
    }
    public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(table)
                        .setUsername(Constant.DORIS_NAME)
                        .setPassword(Constant.DORISL_PASSWORD)
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        .setLabelPrefix(labelPrefix+System.currentTimeMillis())  // stream-load 导入数据时 label 的前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

}
