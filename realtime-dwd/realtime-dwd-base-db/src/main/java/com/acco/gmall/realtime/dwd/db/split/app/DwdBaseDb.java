package com.acco.gmall.realtime.dwd.db.split.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TableProcessDwd;
import com.acco.gmall.realtime.common.constant.Constant;

import com.acco.gmall.realtime.common.util.FlinkSourceUtil;
import com.acco.gmall.realtime.common.util.JdbcUtil;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * ClassName: DwdBaseDb
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.split.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-07 12:17
 */
public class DwdBaseDb extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,4, "dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        // 1.已经从kafka里面读取了topic_db的数据
//        kafkaSourceStream.print();
        // 2.清洗数据
        SingleOutputStreamOperator<JSONObject> jsonObjSteam = kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            System.out.println("被清洗的脏数据:" + value);
                        }
                    }
                }
        );
        // 从flink-cdc读取配置表信息
        DataStreamSource<String> table_process_dwd = env.fromSource(
                FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWM_TABLE_NAME),
                WatermarkStrategy.noWatermarks(),
                "table_process_dwd").setParallelism(1);

        // 转换数据格式

        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = table_process_dwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd tableProcessDwd;

                    if ("d".equals(op)) {
                        tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    tableProcessDwd.setOp(op);
                    out.collect(tableProcessDwd);
                } catch (Exception e) {
                    System.out.println("flink-cdc脏数据:" + value);
                }
            }
        }).setParallelism(1);
//        processDwdStream.print();
        // 广播流,将dwd维度信息广播出去
        MapStateDescriptor<String, TableProcessDwd> mapState = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapState);
        // 连接主流和广播流 对主流进行判断,是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processSteam = jsonObjSteam.connect(broadcastStream).process(

                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection,
                                "select * from gmall2023_config.table_process_dwd",
                                TableProcessDwd.class, true);
//                        System.out.println(tableProcessDwds);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
//                            System.out.println(tableProcessDwd);
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }

                    }
                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        // 主要是写
                        // 判断当前广播状态是否需要保留
                        String table = value.getString("table");
                        String type = value.getString("type");
                        String key = table + ":" + type;
//                        System.out.println(key);
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapState);
                        TableProcessDwd procesDwd = broadcastState.get(key);
                        if (procesDwd == null) {
                            procesDwd = hashMap.get(key);
                        }
//                        System.out.println(procesDwd);
                        if (procesDwd != null) {
                            out.collect(Tuple2.of(value, procesDwd));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        // 主要是读 将配置表中的数据存放到广播状态中
                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapState);
                        String op = value.getOp();
                        String key = value.getSourceTable() + ":" + value.getSourceType();
                        if ("d".equals(op)) {
                            broadcastState.remove(key);
                        } else {
                            broadcastState.put(key, value);
                            hashMap.remove(key);
                        }
                    }
                }
        );
//        processSteam.print();
        // 筛选出需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataSteam = processSteam.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
                    @Override
                    public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                        JSONObject f0 = value.f0;
                        TableProcessDwd f1 = value.f1;
                        JSONObject data = f0.getJSONObject("data");
                        List<String> columns = Arrays.asList(f1.getSinkColumns().split(",")); // 获取列明
                        data.keySet().removeIf(key -> !columns.contains(key)); // 如果不包含 就remove 不是所有字段都需要传到kafka
                        data.put("sink_table", f1.getSinkTable());
                        System.out.println(f1.getSinkTable());
                        return data;

                    }
                }
        );
//        dataSteam.print();
        dataSteam.sinkTo(KafkaSink.<JSONObject>builder()
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
                .build()
        );


    }
}
