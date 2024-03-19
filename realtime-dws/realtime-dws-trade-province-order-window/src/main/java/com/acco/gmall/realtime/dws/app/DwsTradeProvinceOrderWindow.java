package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.AsyncDimFunction;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeProvinceOrderWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-17 22:24
 */
public class DwsTradeProvinceOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL

        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        SingleOutputStreamOperator<String> etlStream = getEtlStream(stream);
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = etlStream
                .map(new MapFunction<String, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        HashSet<String> set = new HashSet<>();
                        set.add(obj.getString("order_id"));

                        return TradeProvinceOrderBean.builder()
                                .orderDetailId(obj.getString("id"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .provinceId(obj.getString("province_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .orderIdSet(set)
                                .build();
                    }

                })
                .keyBy(TradeProvinceOrderBean::getOrderDetailId)  // 按照详情 id 去重
                .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isFirst", Boolean.class));
                        // 一定给状态添加 ttl 省略
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean value,
                                               Context ctx,
                                               Collector<TradeProvinceOrderBean> out) throws Exception {
                        // 因为后期需要聚合的数据都在左表, 所以,可以只去当前详情 id 的第一条数据即可
                        if (isFirstState.value() == null) {
                            isFirstState.update(true);
                            out.collect(value);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(TradeProvinceOrderBean::getProvinceId) // 分组开窗聚合
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1,
                                                                 TradeProvinceOrderBean value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String provinceId,
                                                Context ctx,
                                                Iterable<TradeProvinceOrderBean> elements,
                                                Collector<TradeProvinceOrderBean> out) throws Exception {
                                TradeProvinceOrderBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                                bean.setOrderCount((long) bean.getOrderIdSet().size());
                                out.collect(bean);
                            }
                        }
                );

        AsyncDataStream
                .unorderedWait(  // 异步的方式补充维度
                        reducedStream,
                        new AsyncDimFunction<TradeProvinceOrderBean>() {
                            @Override
                            public String getId(TradeProvinceOrderBean bean) {
                                return bean.getProvinceId();
                            }

                            @Override
                            public String getTableName() {
                                return "dim_base_province";
                            }

                            @Override
                            public void join(TradeProvinceOrderBean bean,
                                                JSONObject dim) {
                                bean.setProvinceName(dim.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + ".dws_trade_province_order_window", "dws_trade_province_order_window"));


    }





    private SingleOutputStreamOperator<String> getEtlStream(DataStreamSource<String> kafkaSourceStream) {
        return kafkaSourceStream.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            String id = jsonObject.getString("id");
                            String order_id = jsonObject.getString("order_id");
                            String province_id = jsonObject.getString("province_id");
                            Long ts = jsonObject.getLong("ts");
                            if (id != null || order_id != null || province_id != null) {
                                jsonObject.put("ts", ts * 1000);
                                out.collect(value);
                            }

                        } catch (Exception e) {
                            System.out.println("脏数据:" + value);
                        }
                    }
                }
        );
    }
}
