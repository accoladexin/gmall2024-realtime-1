import com.acco.gmall.realtime.common.base.BaseAPP;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.prefs.BackingStoreException;

/**
 * ClassName: testkafkasin
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-07 15:03
 */
public class testkafkasin extends BaseAPP {
    public static void main(String[] args) {
        new testkafkasin().start(10032,9,"1111","dwd_user_register");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSourceStream) {
        kafkaSourceStream.print();
    }
}
