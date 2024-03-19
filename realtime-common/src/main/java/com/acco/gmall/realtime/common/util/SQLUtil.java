package com.acco.gmall.realtime.common.util;

import com.acco.gmall.realtime.common.constant.Constant;

/**
 * ClassName: SQLUtil
 * Description: None
 * Package: com.acco.gmall.realtime.common.util
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-04 23:02
 */
public class SQLUtil {
    public static String getKafkaDDLSource(String topic, String groupId) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";

    }
    public static String getKafkaTopicDb(String groupID){
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "   `proc_time` as PROCTIME(),\n"+ // 处理时间
                "   order_time as TO_TIMESTAMP_LTZ(ts, 3),\n" + // event time
                "WATERMARK FOR order_time AS order_time - INTERVAL '15' SECOND\n" + // 水位线
                ") " +getKafkaDDLSource(Constant.TOPIC_DB,groupID);
    }

    public static String getKafkaDDLSink(String topicName) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topicName + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * 获取upsert kafka连接,创建语句一定要有主键
     * @param topicName
     * @return
     */
    public static  String getUpsertKafkaSink(String topicName){
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topicName+"',\n" +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }


    public static String getDorisSinkSQL(String tableName){
        return  "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '"+Constant.FENODES+"',\n" +
                "      'table.identifier' = '"+Constant.DORIS_DATABAES+"."+tableName+"',\n" +
                "      'username' = '"+Constant.DORIS_NAME+"',\n" +
                "      'password' = '"+Constant.DORISL_PASSWORD+"',\n" +
                "      'sink.label-prefix' = 'doris_label"+System.currentTimeMillis()+"'\n" +
                ")";
    }

}

