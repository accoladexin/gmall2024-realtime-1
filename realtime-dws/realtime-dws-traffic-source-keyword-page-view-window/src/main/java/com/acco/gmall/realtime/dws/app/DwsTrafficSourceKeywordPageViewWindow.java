package com.acco.gmall.realtime.dws.app;

import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.SQLUtil;
import com.acco.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Description: None
 * Package: com.acco.gmall.realtime.dws.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-09 23:09
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4, "dws_traffic_source_keyword_page_view_window");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {

        tEnv.executeSql("create table page_info(\n" +
                "    `common` map<string,string>,\n" +
                "    `page` map<string,string>,\n" +
                "    `ts` bigint,\n" +
                "`row_time` as TO_TIMESTAMP_LTZ(ts, 3)," +
                " WATERMARK FOR row_time AS row_time - INTERVAL '1' MINUTE"+
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRAFFIC_PAGE,ckAndGroupId));
        Table keywrodsTable = tEnv.sqlQuery("select \n" +
                "    page['item'] keywords,\n" +
                "    `ts` ,\n" +
                " `row_time` " +
                "from page_info  \n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null\n");

        //1 读取dwd页面主题的数据

        // 2.筛选出关键字keywords
        tEnv.createTemporaryView("keywords_table",keywrodsTable); //注册

        //3.自定义udf函数 然后注册
        tEnv.createTemporarySystemFunction("SplitFunction", KwSplit.class); //第一是名字,第一是自定义函数的类


        // 4.使用分词函数对keyword进行拆分
        Table keySplitTable = tEnv.sqlQuery(
                "SELECT keywords, keyword,ts,`row_time` " + //列明在自定义函数类的注解哪里
                        "FROM keywords_table " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(keywords)) ON TRUE");

        tEnv.createTemporaryView("key_split_table",keySplitTable);

        Table resultTable = tEnv.sqlQuery("SELECT\n" +
                "  cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) stt,\n" +
                "  cast(TUMBLE_END(row_time, INTERVAL '10' SECOND) as STRING) edt, " +
                "  cast(current_date as string) cur_date," +
                "  keyword,\n" +
                "  count(*) keyword_count\n" +
                "   FROM key_split_table\n" +
                "   GROUP BY\n" +
                "  TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "  keyword");

        // 5.对keyword进行开窗聚合

        // 写出到doris //必须打开检查点
        //建表
        tEnv.executeSql("CREATE TABLE flink_doris_sink (\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING,\n" +
                "    keyword STRING,\n" +
                "    keyword_count bigint\n" +
                "    ) \n" + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
        System.out.println(SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        resultTable.insertInto("flink_doris_sink").execute();

    }
}
