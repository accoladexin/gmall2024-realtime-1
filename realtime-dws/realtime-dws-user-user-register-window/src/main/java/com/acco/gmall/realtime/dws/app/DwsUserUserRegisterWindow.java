package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.UserRegisterBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: DwsUserUserRegisterWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-15 14:00
 */
public class DwsUserUserRegisterWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(
                10025,
                4,
                "dws_user_user_register_window",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("create_time")) // fastjson 会自动把 datetime 转成 long
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject value, Long acc) {
                                return acc + 1;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc;
                            }

                            @Override
                            public Long merge(Long acc1, Long acc2) {
                                return acc1 + acc2;
                            }
                        },
                        new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<Long> elements,
                                                Collector<UserRegisterBean> out) throws Exception {
                                Long result = elements.iterator().next();

                                out.collect(new UserRegisterBean(DateFormatUtil.tsToDateTime(ctx.window().getStart()),
                                        DateFormatUtil.tsToDateTime(ctx.window().getEnd()),
                                        DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()),
                                        result
                                ));

                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + "."+Constant.DWS_USER_USER_REGISTER_WINDOW, "dws_user_user_register_window"));

    }
}
