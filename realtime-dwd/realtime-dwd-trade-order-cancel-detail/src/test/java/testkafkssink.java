import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: testkafkssink
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 20:34
 */
public class testkafkssink extends BaseAPP {
    public static void main(String[] args) {
        new testkafkssink().start(10041,4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();
    }
}
