package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.UserLoginBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsUserUserLoginWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-14 17:04
 */
public class DwsUserUserLoginWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE

        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
//        kafkaSourceStream.print();
        //核心逻辑
        // 对数据清洗过滤 uuid不能为空
        SingleOutputStreamOperator<JSONObject> jsonObjSteam = etl(kafkaSourceStream);


        // 注册水位线
        SingleOutputStreamOperator<JSONObject> waterSteam = getTs(jsonObjSteam);

        // 按照uid分组

        KeyedStream<JSONObject, String> keyedSteam = getKeyedSteam(waterSteam);

        // 判断独立用户和回流用户

        SingleOutputStreamOperator<UserLoginBean> procesSteam = ProcessStream(keyedSteam);
//        procesSteam.print();

        // 开窗 + 聚合
        SingleOutputStreamOperator<Object> reduceSteam = getReduce(procesSteam);
//        reduceSteam.print();

        reduceSteam.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES+"."+Constant.DWS_USER_USER_LOGIN_WINDOW,
                        Constant.DWS_USER_USER_LOGIN_WINDOW));

    }

    private SingleOutputStreamOperator<Object> getReduce(SingleOutputStreamOperator<UserLoginBean> procesSteam) {
        return procesSteam.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        long result1 = value1.getBackCt() + value2.getBackCt();
                        value1.setBackCt(result1);
                        long result2 = value1.getUuCt() + value2.getUuCt();
                        value1.setUuCt(result2);
                        return value1;
                    }

                }, new ProcessAllWindowFunction<UserLoginBean, Object, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<UserLoginBean> elements, Collector<Object> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDate(System.currentTimeMillis());
                        for (UserLoginBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }


                    }
                });
    }

    private SingleOutputStreamOperator<UserLoginBean> ProcessStream(KeyedStream<JSONObject, String> keyedSteam){
        return keyedSteam.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastloginDtDesc = new ValueStateDescriptor<String>("last_log_dt", String.class);
                lastLoginState = getRuntimeContext().getState(lastloginDtDesc);
                // 不用设置状态有效时间,因为如何访客数 存不了的话,那设备该升级了
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                String lastLoginDt = lastLoginState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                Long backCt = 0L; // 回流数
                Long uuCt = 0L; // 独立用户数
                if (lastLoginDt == null) {
                    uuCt = 1L;
                    lastLoginState.update(curDt);
                } else if (ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000) {
                    // 回流用户
                    backCt = 1L;
                    uuCt = 1L;
                    lastLoginState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {
                    // 之前有登陆,但是不是今天
                    uuCt = 1L;
                    lastLoginState.update(curDt);
                } else {
                    // 状态不为空 今天有有一次登陆
                }
                if(uuCt != 0){
                    // 不是独立用户,肯定不是回流用户
                    out.collect(new UserLoginBean("","","",backCt,uuCt,ts));

                }



            }
        });

    }

    private KeyedStream<JSONObject, String> getKeyedSteam(SingleOutputStreamOperator<JSONObject> waterSteam) {
        return waterSteam.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String uid = value.getJSONObject("common").getString("uid");
                return uid;
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> getTs(SingleOutputStreamOperator<JSONObject> jsonObjSteam) {
        return jsonObjSteam.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        ));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSourceStream) {
        return kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            JSONObject common = jsonObject.getJSONObject("common");
                            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                            Long uid = common.getLong("uid");
                            Long ts = jsonObject.getLong("ts");
                            if (uid != null && ts != null && (lastPageId == null || "login".equals(lastPageId))) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("清洗掉的数据:" + value);
                        }

                    }
                }
        );
    }
}
