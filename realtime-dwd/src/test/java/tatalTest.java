import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: tatalTest
 * Description: None
 * Package: PACKAGE_NAME
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-04 18:11
 */
public class tatalTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(10L));
        tableEnvironment.executeSql("CREATE TABLE base_dic (\n" +
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
        tableEnvironment.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "   `proc_time` as PROCTIME()\n"+ // 处理时间
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup1',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")\n");
//        tableEnvironment.executeSql("select * from base_dic");
//        tableEnvironment.executeSql("select * from topic_db");
        Table comment_info = tableEnvironment.sqlQuery("select \t\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['nick_name'] nick_name,\n" +
                "\t`data`['head_img'] head_img,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['spu_id'] spu_id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['appraise'] appraise,\n" +
                "\t`data`['comment_txt'] comment_txt,\n" +
                "\t`data`['operate_time'] operate_time,\n" +
                "\t`data`['create_time'] create_time,\n" +
                        "\tproc_time \n"+
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'comment_info'\n" +
                "and `type` = 'insert'");
        tableEnvironment.createTemporaryView("comment_info",comment_info);

        tableEnvironment.executeSql("select \n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tnick_name,\n" +
                "\tsku_id,\n" +
                "\tspu_id,\n" +
                "\torder_id,\n" +
                "\tappraise,\n" +
                "\tdic_name,\n" +
                "\tcomment_txt,\n" +
                "\tcom.operate_time,\n" +
                "\tcom.create_time\n" +
                "from \n" +
                "\tcomment_info com\n" +
                "join base_dic FOR SYSTEM_TIME AS OF com.proc_time AS ba\n" +
                "on ba.dic_code = com.appraise").print();
    }
}
