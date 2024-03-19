import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 * ClassName: testkafsink
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-07 12:13
 */
public class testkafsink extends BaseAPP {
    public static void main(String[] args) {
        new testkafsink().start(10032,4,"test1", Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();
    }
}
