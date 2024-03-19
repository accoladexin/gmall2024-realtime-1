import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.jar.JarEntry;

/**
 * ClassName: test
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-15 16:59
 */
public class test extends BaseAPP {
    public static void main(String[] args) {
        new test().start(10042,4, "dws_trade_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        SingleOutputStreamOperator<JSONObject> map = kafkaSourceStream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
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
                }


        );
        KeyedStream<JSONObject, String> user_id = map.keyBy(obj -> obj.getString("user_id"));
        SingleOutputStreamOperator<JSONObject> process = user_id.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<Integer> count11;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
//                        StateTtlConfig buildConfig = StateTtlConfig.newBuilder(Time.seconds(60 * 60L*1000))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).build();
                        ValueStateDescriptor<Integer> count1 = new ValueStateDescriptor<Integer>("count", Types.INT);
//                        count1.enableTimeToLive(buildConfig);
                        count11 = getRuntimeContext().getState(count1);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (count11.value() == null) {
                            count11.update(0);
                            if (ctx.getCurrentKey().equals("221")){
                                System.out.println("intial:" + count11.value());
                            }
                        }
                        Integer value1 = count11.value();

                        count11.update(value1 + 1);
                        if ( ctx.getCurrentKey().equals("221")) {
//                            System.out.println(value.getString("user_id"));
                            System.out.println(ctx.getCurrentKey() + ":" +value1+"->"+ count11.value());

                        }

                    }
                }
        );
    }
}
