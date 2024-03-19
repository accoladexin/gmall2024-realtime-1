package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.function.AsyncDimFunction;
import com.acco.gmall.realtime.common.function.DorisMapFunction;
import com.acco.gmall.realtime.common.util.DateFormatUtil;
import com.acco.gmall.realtime.common.util.FlinkSinkUtil;
import com.acco.gmall.realtime.common.util.HBaseUtil;
import com.acco.gmall.realtime.common.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ClassName: DwsTradeSkuOrderWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-17 16:01
 */
public class DwsTradeSkuOrderWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL

        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
//        kafkaSourceStream.print();
        // etl
        SingleOutputStreamOperator<JSONObject> etlSteam = getEtlSteam(kafkaSourceStream);
//        etlSteam.print("etl:");

        // 添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = getWaterStream(etlSteam);

        // 修正度量值 考虑上游的数据有测回流
        // 先按照id keyby
        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("id");
                    }
                }
        );//然后聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = getprocessStream(keyedStream);
//        processStream.print("processStream<<");

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = getReduceStream(processStream);
//        reduceStream.print();

        // 关联维度信息 异步IO
        // 关联信息
        // 从habase里面读取数据
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = getFullDimInfo(reduceStream);
        c1Stream.print();

        // 写出到dori里面

        c1Stream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABAES+"."+Constant.DWS_TRADE_SKU_ORDER_WINDOW,"dws_trade_sku_order_window"));
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> getFullDimInfo(SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(
                reduceStream,
                new AsyncDimFunction<TradeSkuOrderBean>() { // 关联的表名字
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getSkuId(); // 匹配的字段
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimSkuInfo) { // 填充字段
                        input.setCategory3Id(dimSkuInfo.getString("category3_id"));
                        input.setTrademarkId(dimSkuInfo.getString("tm_id"));
                        input.setSpuId(dimSkuInfo.getString("spu_id"));
                        input.setSkuName(dimSkuInfo.getString("sku_name"));
                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS); //超时时间为60s
        skuInfoStream.print();
//        SingleOutputStreamOperator<TradeSkuOrderBean> mapStream = getmapStream(reduceStream);
//        mapStream.print();

        // 异步IO关联到spu
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getSpuId();
                    }
                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }
                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dim) {
                        input.setSpuName(dim.getString("spu_name"));
                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS);//超时时间为60s),
//        spuInfoStream.print();

        // 异步io关联 品牌

        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = AsyncDataStream.unorderedWait(spuInfoStream,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dim) {
                        input.setTrademarkName(dim.getString("tm_name"));

                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS);//超时时间为60s),)
//        tmStream.print();


        // 关联c3
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(tmStream,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory3Name(dim.getString("name"));
                        input.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS);//超时时间为60s),)

        // 关联c2
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory2Name(dim.getString("name"));
                        input.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS);//超时时间为60s),)


        //关联c1
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = AsyncDataStream.unorderedWait(c2Stream,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getId(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory1Name(dim.getString("name"));

                    }
                },
                60,  // 超时时间
                TimeUnit.SECONDS);//超时时间为60s),)
        return c1Stream;
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> getReduceStream(SingleOutputStreamOperator<TradeSkuOrderBean> processStream) {
        return processStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean value) throws Exception {
//                        System.out.println(111);
                        return value.getSkuId();
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
//                        System.out.println(11111111);
//                        System.out.println(value2);
                        boolean flge = false;
                        if (flge) {
                            System.out.println(value1.getOrderAmount() + ":" + (value2.getOrderAmount()));
                            System.out.println(value1);
                        }
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        for (TradeSkuOrderBean element : elements) {
                            element.setStt(DateFormatUtil.tsToDateTime(start));
                            element.setEdt(DateFormatUtil.tsToDateTime(end));
                            element.setCurDate(DateFormatUtil.tsToDate(System.currentTimeMillis()));
//                            System.out.println(element);
                            out.collect(element);
                        }

                    }
                });
    }


    private SingleOutputStreamOperator<TradeSkuOrderBean> getmapStream(SingleOutputStreamOperator<TradeSkuOrderBean> reduceSteam) {
        return reduceSteam.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            // 异步关联维度信息
            Connection connection;
            Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
//                System.out.println(11111111111111);
                connection = HBaseUtil.getConnection();
                jedis = RedisUtil.getJedis();

            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
                RedisUtil.closeJedis(jedis);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
                // 拼接对应redis key
                String redisKey = RedisUtil.getRedisKey("dim_sku_info", value.getSkuId());
                // 读取redis缓存的数据 Json
                String dim = jedis.get(redisKey);
                // 判断是否为空 redis
                JSONObject dimSkuInfo;
                boolean flag = false; // 打印的标记
//                System.out.println(value);
                if (dim == null || dim.length() == 0) {
                    // redis没有,需要到hbase里面读取

                    dimSkuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", value.getSkuId());
                    // 存到redis
                    if (dimSkuInfo.size() != 0) {
                        // 第一个是表 第二个ttl 1day 第三;数据
                        jedis.setex(redisKey, 24 * 60 * 60, dimSkuInfo.toJSONString());
                    }
                    if (flag) {
                        System.out.println("没有缓存:" + redisKey + "\nHbase data: " + dimSkuInfo);
                    }
                } else {
                    //redis里面有值
                    dimSkuInfo = JSONObject.parseObject(dim);
                    if (flag) {
                        System.out.println("有缓存:" + redisKey + "\ndata:" + dim);
                    }
                }
                if (dimSkuInfo.size() != 0) {
                    // 维度关联
                    value.setCategory3Id(dimSkuInfo.getString("category3_id"));
                    value.setTrademarkId(dimSkuInfo.getString("tm_id"));
                    value.setSpuId(dimSkuInfo.getString("spu_id"));
                    value.setSkuName(dimSkuInfo.getString("sku_name"));
                } else {
                    System.out.println("没有值:" + redisKey);
                }
                return value;
            }
        });

    }


    private SingleOutputStreamOperator<TradeSkuOrderBean> getprocessStream(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;
//            ValueState<Integer> count;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountStateDes = new MapStateDescriptor<>("lastAmountState", Types.STRING, Types.BIG_DEC);
                lastAmountStateDes.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountStateDes);
//                count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("11", Types.INT));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                // 获取值
                BigDecimal originalAmount = lastAmountState.get("OriginalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");
                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

//                count.update(count.value()==null?0:count.value()+1);
//                if (count.value()>2){
//                    System.out.println(ctx.getCurrentKey());
//                }

                BigDecimal curOriginalAmount = value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num"));
                //每一条id相同的值减去上一条的值
                TradeSkuOrderBean build = TradeSkuOrderBean.builder()
                        .skuId(value.getString("sku_id"))
                        .orderDetailId(value.getString("id"))
                        .ts(value.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(value.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(value.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(value.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();
//                System.out.println(build);
                out.collect(build);
                boolean flag = false; // 只用于用于调试
                if (ctx.getCurrentKey().equals("3307") && flag == true) {
                    System.out.println("originalAmount" + ":" + originalAmount);
                    System.out.println("activityReduceAmount" + ":" + activityReduceAmount);
                    System.out.println("couponReduceAmount" + ":" + couponReduceAmount);
                    System.out.println("orderAmount" + ":" + orderAmount);
                    System.out.println(build);
                    System.out.println("=======================");
                }

                // 存取当前的数据
                lastAmountState.put("OriginalAmount", curOriginalAmount);
                lastAmountState.put("activityReduceAmount", value.getBigDecimal("split_total_amount"));
                lastAmountState.put("couponReduceAmount", value.getBigDecimal("split_activity_amount"));
                lastAmountState.put("orderAmount", value.getBigDecimal("split_activity_amount"));
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> getWaterStream(SingleOutputStreamOperator<JSONObject> etlSteam) {
        return etlSteam.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                           @Override
                                           public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                               return element.getLong("ts");
                                           }
                                       }
                )

        );
    }


    private SingleOutputStreamOperator<JSONObject> getEtlSteam(DataStreamSource<String> kafkaSourceStream) {
        return kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            if (value != null) {
                                JSONObject jsonObject = JSONObject.parseObject(value);
                                Long ts = jsonObject.getLong("ts");
                                String id = jsonObject.getString("id");
                                String sku_id = jsonObject.getString("sku_id");
                                if (ts != null && id != null && sku_id != null) {
                                    jsonObject.put("ts", ts * 1000); // 修正水位线
                                    out.collect(jsonObject);
                                }
                            }
                        } catch (Exception e) {
                            System.out.println("脏数据:" + value);
                        }


                    }
                }
        );
    }
}
