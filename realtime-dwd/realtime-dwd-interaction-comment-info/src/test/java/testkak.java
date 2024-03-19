import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: testkak
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-05 20:42
 */
public class testkak extends BaseAPP {

    public static void main(String[] args) {
        new testkak().start(10019,4,"test", Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();

    }
}
