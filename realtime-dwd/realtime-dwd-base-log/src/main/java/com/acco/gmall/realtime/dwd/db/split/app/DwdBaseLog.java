package com.acco.gmall.realtime.dwd.db.split.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.awt.print.Printable;
import java.time.Duration;

/**
 * ClassName: DwdBaseLog
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.split.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-27 16:10
 */
public class DwdBaseLog extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
         kafkaSourceStream.print("kafkaSourceStream");
        // TODO 数据清洗 etl
        SingleOutputStreamOperator<JSONObject> jsonObjSteam = etl(kafkaSourceStream);
//        jsonObjSteam.print("etl");
        // TODO 注册水位线和keyby
        KeyedStream<JSONObject, String> keybyStream = keyByWithWaterMark(jsonObjSteam);
//        keybyStream.print("keyby");
        //TODO 2）新老访客状态标记修复
        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keybyStream);
        // TODO 3）分流：利用侧输出流实现数据拆分
//        process.print();
        // 启动日志 :启动信息 报错信息
        // 和页面日 :页面信息 曝光信息 动作信息 报错信息
        OutputTag<String> startTag = new OutputTag<String>("start"){}; // 要加{},否则会泛型擦除 或者下面的写法
        OutputTag<String> errorTag = new OutputTag<>("error", TypeInformation.of(String.class)); //指定泛型信息,和上面那种二选一
        OutputTag<String>  displayTag = new OutputTag<>("display", TypeInformation.of(String.class)); //指定泛型信息,和上面那种二选一
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class)); //指定泛型信息,和上面那种二选一
//        OutputTag<String> page = new OutputTag<>("page", TypeInformation.of(String.class)); //指定泛型信息,和上面那种二选一

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        pageStream.print("主流(page):");
        startStream.print("startStream:");
        errorStream.print("erorrStream:");
        actionStream.print("actionStream:");
        displayStream.print("displayStream:");
        // 不同的流写到不同的kafka主题里面去
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));



    }

    private SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 根据数据不同,写到不同的流
                JSONObject error = value.getJSONObject("err");
                if (error != null) {
//                    System.out.println(error);
                    ctx.output(errorTag, error.toJSONString());
                    value.remove("err");
                }
                //其他信息
                JSONObject page = value.getJSONObject("page");
                JSONObject start = value.getJSONObject("start");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");
                if (start != null) {
                    // 启动日志
                    ctx.output(startTag, start.toJSONString());
                } else if (page != null) {
                    // 不是启动日志 那就是页面日志
                    JSONArray displays = value.getJSONArray("displays");
                    if(displays != null){
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
//                        System.out.println(display);
                            display.put("common", common);
//                        System.out.println(display);
//                        System.out.println("-----");
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayTag, display.toString());
                        }
                    }

                    value.remove("display");
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null){
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    value.remove("action");
                    // 只保留page信息,写到主流里面
                    out.collect(value.toJSONString());

                } else {
                    // 留空
                }

            }
        });
    }


    public SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keybyStream){
        return keybyStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> firstLoginDtState;
                    @Override
                    public void open(Configuration parameters) throws Exception { // 运行的次数和并行度一样
//                        firstLoginDtState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<String>("first_login_dt", String.class)
//                        ); // 只知道这是每台设备存第一次登陆时间
                        firstLoginDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("first_login_dt", String.class)
                        ); // 只知道这是每台设备存第一次登陆时间
//                        System.out.println(1);
//                        System.out.println(null);
//                        String valueFirstLogin = firstLoginDtState.value();
//                        System.out.println(valueFirstLogin.toString());
//                        System.out.println(firstLoginDtState.value()); // 这行代码有鬼
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        // 获取当前is_new字段
                        JSONObject common = value.getJSONObject("common");
                        String is_new = common.getString("is_new");
                        String valueFirstLogin = firstLoginDtState.value();
//                        System.out.println(valueFirstLogin);
                        Long ts = value.getLong("ts");
                        // 时间戳转化格式
                        String curDt = DateFormatUtil.tsToDate(ts);
                        if ("1".equals(is_new)) {
                            // 判断当前zhuantai
                            if (valueFirstLogin != null && !valueFirstLogin.equals(curDt)) {
                                // 当前状态不为空 日期也不是今天 伪装的新访问可
                                common.put("is_new", "0");
                            } else if (valueFirstLogin == null) {
                                firstLoginDtState.update(curDt);
                            } else {
                                // 留空
                            }
                        } else {
                            if (valueFirstLogin == null) {
                                // 老用户,但是Flink上线晚,需要更新
                                firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                            }
                        }
                        out.collect(value);

                    }
                }
        );
    }

    private KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjSteam) {
        return jsonObjSteam.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }
                )
        ).keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {

                        return value.getJSONObject("common").getString("mid");
                    }
                }
        );
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSourceStream) {
        return kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {

                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            JSONObject page = jsonObject.getJSONObject("page");
                            Object start = jsonObject.getJSONObject("start");
                            JSONObject common = jsonObject.getJSONObject("common");
                            Long ts = jsonObject.getLong("ts");
                            if (page != null || start != null) {
                                if (common != null && common.getString("mid") != null && ts != null) { //保证后续流可以执行
                                    out.collect(jsonObject);
                                }
                            }

                        } catch (Exception e) {
                            System.out.println("过滤掉的脏数据:" + value);
                        }
                    }
                }
        );
    }
}
