package com.acco.gmall.realtime.dim.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TableProcessDim;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.FlinkSourceUtil;
import com.acco.gmall.realtime.common.util.HBaseUtil;
import com.acco.gmall.realtime.common.util.JdbcUtil;
import com.acco.gmall.realtime.dim.function.DimHBaseSinkFunction;
import com.acco.gmall.realtime.dim.function.dimBroadcastFunction;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import io.debezium.embedded.Connect;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.JarUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hbase.client.Connection;


import java.awt.print.Printable;
import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

/**
 * ClassName: dimApp
 * Description: None
 * Package: com.acco.gmall.realtime.dim.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-22 19:55
 */
public class DimApp extends BaseAPP {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑 数据处理
        // 从mysql同步维度表同步到
        // TODO 对ods出来的数据进行处理
        /*stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                boolean flat = false;
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database)
                            && !"bootstrap-start".equals(type) && "bootstrap-complete".equals(type)// type除了crud 还有这两种类型,需要排除
                            && data != null && data.size() != 0)
                        System.out.println(database);
                    flat = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return flat;
            }
        }).map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        }); */

        SingleOutputStreamOperator<JSONObject> jsonStream = etl(stream);
        // TODO 用Mysql读维度数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> mysql_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1); // 一定要为1,否则抓取不对
//        mysql_source.print();
        // TODO 在Hbase建表
        /**
         {
         "before":null,
         "after":{
         "source_table":"base_trademark",
         "sink_table":"dim_base_trademark",
         "sink_family":"info",
         "sink_columns":"id,tm_name",
         "sink_row_key":"id"
         },
         "source":{
         "version":"1.9.7.Final",
         "connector":"mysql",
         "name":"mysql_binlog_source",
         "ts_ms":0,
         "snapshot":"false",
         "db":"gmall2023_config",
         "sequence":null,
         "table":"table_process_dim",
         "server_id":0,
         "gtid":null,
         "file":"",
         "pos":0,
         "row":0,
         "thread":null,
         "query":null
         },
         "op":"r",
         "ts_ms":1708841758126,
         "transaction":null
         } */
        SingleOutputStreamOperator<TableProcessDim> HbaseStream = createHbaseTable(mysql_source).setParallelism(1);
        // TODO 做成广播流
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state",
                String.class, TableProcessDim.class);// broadcast_state应该是个名字(标记),后面是名名字(标记)的类型
        BroadcastStream<TableProcessDim> broadcastStream = HbaseStream.broadcast(broadcastState);
        // TODO 链接主流和广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = jsonStream.connect(broadcastStream);

        // ToDo 处理广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectedStream.process(
                new dimBroadcastFunction(broadcastState)

        );
        // TODO 筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumsStream = getData(dimStream);
        filterColumsStream.print("filterColumsStream: ");
        // TODO 写出到Hbase
        DataStreamSink<Tuple2<JSONObject, TableProcessDim>> tuple2DataStreamSink = filterColumsStream.addSink(
                new DimHBaseSinkFunction() );


    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> getData(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(
                new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                        JSONObject jsonObj = value.f0; // 主流数据
                        TableProcessDim dim = value.f1; //dim维度数据 相互已经匹配了，是一个表格的
                        String sinkColumns = dim.getSinkColumns();
                        // [id, login_name, name, user_level, birthday, gender, create_time, operate_time]
                        List<String> colums = Arrays.asList(sinkColumns.split(","));
                        /**{
                         "birthday": "1992-04-18",
                         "gender": "M",
                         "create_time": "2024-02-18 22:47:58",
                         "login_name": "y3vf93",
                         "nick_name": "阿明",
                         "name": "单于明",
                         "user_level": "2",
                         "phone_num": "13133987445",
                         "id": 489,
                         "email": "y3vf93@qq.com" // 这个字段没有，会被删除掉
                         }*/
                        JSONObject data = jsonObj.getJSONObject("data");
//                        System.out.println("data:" + data);
//                        System.out.println("colums:" + colums);
                        // 在dim建表信息中，有的字段被删除掉了，应为不是所有的mysql中字段都需要保留
                        data.keySet().removeIf(key -> !colums.contains(key)); // ture见删除，即不再dim建表信息中的都要刷出掉
                        return value;
                    }
                }
        );
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(DataStreamSource<String> mysql_source) {
        SingleOutputStreamOperator<TableProcessDim> HbaseStream = mysql_source.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 获取连接;
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                // TODO 关闭连接
                HBaseUtil.closeConnection(connection);
                super.close();
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                // 使用HBase创建与读取数据对应的表格
                try {
                    JSONObject js = JSONObject.parseObject(value);
                    String op = js.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) { // d表示删除
                        dim = js.getObject("before", TableProcessDim.class);
                        //删除一张表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) { // c create r 应该也是新增表格
                        dim = js.getObject("after", TableProcessDim.class);
                        createTalbe(dim);
                    } else { // 应该是修改表格
                        dim = js.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTalbe(dim);
                    }
                    dim.setOp(op); // 添加字段
                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }

            private void createTalbe(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split(",");

                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(connection,Constant.HBASE_NAMESPACE,dim.getSinkTable());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return HbaseStream;
    }

    public static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database)
                            && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)// type除了crud 还有这两种类型,需要排除
                            && data != null && data.size() != 0) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }


}
