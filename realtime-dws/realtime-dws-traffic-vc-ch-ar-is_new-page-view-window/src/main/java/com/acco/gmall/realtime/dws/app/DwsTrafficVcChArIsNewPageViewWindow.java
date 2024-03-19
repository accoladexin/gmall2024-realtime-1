package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TrafficPageViewBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;

/**
 * ClassName: DwsTrafficVcChArIsNewPageViewWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-11 22:31
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
//        kafkaSourceStream.print();
        //清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonStream = etl(kafkaSourceStream);
        // 按照mid分组 是否为独立访客
        KeyedStream<JSONObject, String> keybySteam = jsonStream.keyBy(new KeySelector<JSONObject, String>() {
            // 定义全局状态
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

//        keybySteam.print();
        // 判断是否为独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = keybySteam.process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            ValueState<String> lastLoginDtState; //全局状态
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                // 设置ttl
                lastLoginDtDesc.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);


//                System.out.println(lastLoginDtState.value()); //不能print 否则阻塞
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
//                System.out.println(value);
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.tsToDate(ts);
                String lastLoginDt = lastLoginDtState.value();
//                System.out.println(lastLoginDt);
                Long uvCt = 0L;
                Long svCT = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDate)) {
                    // 独立访客
                    uvCt = 1L;
                    //独立访客还要更新数据
                    lastLoginDtState.update(curDate);

                }
                //判断会话数
                JSONObject page = value.getJSONObject("page");
                String last_page_id = value.getString("last_page_id");
                if (last_page_id == null) {
                    svCT = 1L;
                }
                JSONObject common = value.getJSONObject("common");
//                System.out.println(common);
                out.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCT)
                        .pvCt(1L)
                        .durSum(page.getLong("during_time"))
                        .sid(common.getString("sid"))
                        .ts(ts)
                        .build());

            }
        });
//        beanStream.print();
        // 添加watermark 时间时间为水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(10L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                )
        );

        //按照粒度分组
        KeyedStream<TrafficPageViewBean, String> keyedWaterStream = withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {

            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {

                return value.getVc() + ":" + value.getCh() + ":" + value.getAr() + ":" + value.getIsNew();
            }
        });
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowSteam = keyedWaterStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        //开窗 聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceStream = windowSteam.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                // 将多个元素度量值累加到一起
                // v1 表示累加的结果，第一次调用就是第一元素
                // v2 表示累加的新元素 11
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }

        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {

            @Override
            public void process(String s, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                // 可以调用上下文，1
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                String stt = DateFormatUtil.tsToDateTime(start);
                String sdt = DateFormatUtil.tsToDateTime(end);
                String curdate = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TrafficPageViewBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(sdt);
                    element.setCur_date(curdate);
                    out.collect(element);
                }


            }
        });
//        reduceStream.print("reduceStream<<<<<<");
        // 写道doris
        SingleOutputStreamOperator<String> map = reduceStream.map(new DorisMapFunction<>());
        map.print("map<<");
        map.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + "."+Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW, "dws_traffic_vc_ch_ar_is_new_page_view_window"));


    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSourceStream) {
        return kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            Long ts = jsonObject.getLong("ts");
                            String mid = jsonObject.getJSONObject("common").getString("mid");
                            if (mid != null && ts != null) {
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("清洗的脏数据:" + value);
                        }

                    }
                }
        );
    }


}
