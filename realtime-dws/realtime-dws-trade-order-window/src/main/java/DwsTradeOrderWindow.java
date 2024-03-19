import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TradeOrderBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.time.Duration;

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

/**
 * ClassName: DwsTradeOrderWindow
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-15 14:55
 */
public class DwsTradeOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                10028,
                4,
                "dws_trade_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
//        stream.print();
        SingleOutputStreamOperator<JSONObject> map = stream
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            if (jsonObject.getString("user_di")==null) {
                                out.collect(jsonObject);
                            }
                        }catch (Exception e){
                            System.out.println("脏数据:"+value);
                        }
                    }
                });
        map
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                                .withIdleness(Duration.ofSeconds(1L))
                )
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDateState;
                    private ValueState<Long> count11;

                    @Override
                    public void open(Configuration parameters) {
                        StateTtlConfig buildConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.seconds(60 * 60L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).build();
                        lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", Types.STRING));
                        ValueStateDescriptor<Long> count1 = new ValueStateDescriptor<Long>("count", Types.LONG);
                        count1.enableTimeToLive(buildConfig);
                        count11 = getRuntimeContext().getState(count1);



                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<TradeOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        if (count11.value() == null){
                            count11.update(0L);
                        }
                        count11.update(count11.value() + 1);
                        if (count11.value() >3){
                            System.out.println(value.getString("user_id"));
                            System.out.println(ctx.getCurrentKey() +" : "+ count11.value());
                        }
//
                        String today = DateFormatUtil.tsToDate(ts);
                        String lastOrderDate = lastOrderDateState.value();

                        long orderUu = 0L;
                        long orderNew = 0L;
                        if (!today.equals(lastOrderDate)) {
                            orderUu = 1L;
                            lastOrderDateState.update(today);

                            if (lastOrderDate == null) {
                                orderNew = 1L;
                            }

                        }
                        if (orderUu == 1) {
                            out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1,
                                                         TradeOrderBean value2) {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
//                                System.out.println(value1);
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
//                                TradeOrderBean bean = elements.iterator().next();
//                                System.out.println("111");
                                for (TradeOrderBean bean : elements) {
//                                    System.out.println(bean);
                                    bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                    bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                    bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                                    System.out.println(bean);
                                    out.collect(bean);
                                }


//
                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES + "."+Constant.DWS_TRADE_ORDER_WINDOW, "dws_trade_order_window"));


    }
}

