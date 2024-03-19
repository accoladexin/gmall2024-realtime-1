import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * ClassName: testTopicDB
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 13:41
 */
public class testTopicDB extends BaseAPP {
    public static void main(String[] args) {
        new testTopicDB().start(10061,4, Constant.TOPIC_DWD_TRADE_CART_ADD, "topic_db");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
//        kafkaSourceStream.print();
        kafkaSourceStream.map(
                new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        if (jsonObject.getString("table").equals("order_detail") && jsonObject.getString("type").equals("insert")){
                            System.out.println(jsonObject.getJSONObject("data"));
                        }

                        return null;
                    }
                }
        );
    }
}
