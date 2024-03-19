import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: testMysql
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-02 22:40
 */
public class testMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  create_time TIMESTAMP,\n" +
                "  operate_time TIMESTAMP, \n" +
                "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'base_dic',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ");\n");
        tableEnv.executeSql("select * from base_dic").print();
    }
}
