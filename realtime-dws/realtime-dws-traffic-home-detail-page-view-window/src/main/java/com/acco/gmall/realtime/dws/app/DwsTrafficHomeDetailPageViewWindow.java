package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


import java.time.Duration;

/**
 * ClassName: DwsTrafficHomeDetailPageViewWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-12 23:09
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023,
                4,
                "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {

        // 1. 过滤, 解析成 pojo 类型
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = parseToPojo(kafkaSourceStream);

        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAgg(beanStream);

//        resultStream.print("resultStream<<<");
        // 3. 写出到 doris
        SingleOutputStreamOperator<String> map = resultStream.map(new DorisMapFunction<>());
        map.print("map<<");
        map.sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + "."+Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW, "dws_traffic_home_detail_page_view_window"));


    }

    private void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + "."+Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW, "dws_traffic_home_detail_page_view_window"));
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1,
                                                                        TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TrafficHomeDetailPageViewBean> elements,
                                                Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                                TrafficHomeDetailPageViewBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                out.collect(bean);

                            }
                        }
                );
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeState;
                    private ValueState<String> goodDetailState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("home", String.class));
                        goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetail", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = obj.getJSONObject("page").getString("page_id");

                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        String lastHomeDate = homeState.value();
                        String lastGoodDetailDate = goodDetailState.value();
                        Long homeCt = 0L;
                        Long goodDetailCt = 0L;
                        // 是首页, 并且今天和状态不相等, 则是当天的第一个首页
                        if ("home".equals(pageId) && !today.equals(lastHomeDate)) {
                            homeCt = 1L;
                            homeState.update(today);
                        } else if ("good_detail".equals(pageId) && !today.equals(lastGoodDetailDate)) {
                            goodDetailCt = 1L;
                            goodDetailState.update(today);
                        }

                        if (homeCt + goodDetailCt == 1) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "", "",
                                    "",
                                    homeCt, goodDetailCt,
                                    ts
                            ));
                        }
                    }
                });


    }
}
