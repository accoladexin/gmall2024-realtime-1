import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: testsource
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 12:26
 */
public class testsource extends BaseAPP {
    public static void main(String[] args) {
        new testsource().start(10091,4, Constant.TOPIC_DWD_TRADE_CART_ADD,Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();
    }
}
