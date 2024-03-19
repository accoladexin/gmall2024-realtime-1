package com.acco.gmall.realtime.common.util;

import com.acco.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * ClassName: FlinkSourceUtil
 * Description: None
 * Package: com.acco.gmall.realtime.common.util
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-23 13:57
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupid,String topicName){
        return  KafkaSource.<String>builder()
                        .setBootstrapServers(Constant.KAFKA_BROKERS)
                        .setTopics(topicName)
                        .setStartingOffsets(OffsetsInitializer.earliest())//从最早开始读数据
                        .setGroupId(groupid)
                        .setValueOnlyDeserializer(
                                //SimpleStringSchema 无法序列化null值,会直接报错
                                // 后续DWD会向kfka发送null值 因此需要重写 /
                                // /不能用new SimpleStringSchema()
                                new DeserializationSchema<String>() {
                                    @Override
                                    public String deserialize(byte[] message) throws IOException {
                                        if (message != null && message.length != 0){
                                            return new String(message, StandardCharsets.UTF_8);
                                        }
                                        return "";
                                    }
                                    @Override
                                    public boolean isEndOfStream(String nextElement) {
                                        return false;
                                    }
                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                }
                        )
                        .build();
    }

    public static MySqlSource<String> getMySqlSource(String databaseName,String tableName){
        Properties props = new Properties(); // 这3行防止mysql不能远程用密码连接
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mysqlsource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props) // 防止mysql不能远程连接,和上面3行代码在一起用
                .databaseList(databaseName)
                .tableList(databaseName+"."+tableName)  // 注意这样写 它支持多库多表
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        return mysqlsource;
    }
}
