import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import com.sun.org.apache.xerces.internal.dom.PSVIAttrNSImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: test
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-12 21:29
 */
public class test extends BaseAPP {
    public static void main(String[] args) {
        new test().start(12331,4,"sets", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();
    }
}
