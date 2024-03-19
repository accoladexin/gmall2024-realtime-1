import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: test03
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-02 20:28
 */
public class test03 extends BaseAPP {
    public static void main(String[] args) {
        new test03().start(10011, 4, "dwd_base_log", Constant.TOPIC_DWD_TRAFFIC_ACTION);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        env.setParallelism(8);
        kafkaSourceStream.print("1");
    }
}
