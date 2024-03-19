package com.acco.gmall.realtime.dwd.db.app;

import com.acco.gmall.realtime.common.base.BaseAPP;
import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdInteractionCommentInfo
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-04 22:48
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,"dwd_interaction_comment_inf");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        // TODO 核心逻辑
        // TODO 从Kafka里面读数据
        createTopicDbkafka(ckAndGroupId,tEnv);
        // TODO 从Hbase里见读数据
        createBaseDic(tEnv);
//        tEnv.executeSql("select * from topic_db").print(); //竟然不能写两次查询语句,否则会报错
//        tEnv.executeSql("select * from base_dic").print();
        // TODO 赛选出topic_db 中评论的新增数据
        // 先筛选数据 评论信息新增表格
        filterCommentInfo(tEnv);
        // 使用lookup join连接
        /**tEnv.executeSql("select \n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tnick_name,\n" +
                "\tsku_id,\n" +
                "\tspu_id,\n" +
                "\torder_id,\n" +
                "\tappraise appraise_code,\n" +
                "\tba.info.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tcreate_time,\n" +
                "\tproc_time operate_time\n" +
                "from \n" +
                "\tcomment_info com\n" +
                "join base_dic FOR SYSTEM_TIME AS OF com.proc_time AS ba\n" +
                "on ba.dic_code = com.appraise").print();*/

        // 使用lookup 完成维度退化
        Table joinTable = lookupJoin(tEnv);

        // 创建kafka对应的表格
        getKafkaSInkTable(tEnv);
        // 写出到Kafka主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }

    private void getKafkaSInkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("create table "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "id STRING,\n" +
                "user_id STRING,\n" +
                "nick_name STRING,\n" +
                "sku_id STRING,\n" +
                "spu_id STRING,\n" +
                "order_id STRING,\n" +
                "appraise_code STRING,\n" +
                "appraise_name STRING,\n" +
                "comment_txt STRING,\n" +
                "create_time STRING,\n" +
                "operate_time STRING\n" +
                ")"
        + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    private Table lookupJoin(StreamTableEnvironment tEnv) {
        Table joinTable = tEnv.sqlQuery("select \n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tnick_name,\n" +
                "\tsku_id,\n" +
                "\tspu_id,\n" +
                "\torder_id,\n" +
                "\tappraise appraise_code,\n" +
                "\tba.info.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tcreate_time,\n" +
                "\tCAST(proc_time as String) operate_time\n" +
                "from \n" +
                "\tcomment_info com\n" +
                "join base_dic FOR SYSTEM_TIME AS OF com.proc_time AS ba\n" +
                "on ba.dic_code = com.appraise");
        return joinTable;
    }

    private void filterCommentInfo(StreamTableEnvironment tEnv) {
        Table comment_info = tEnv.sqlQuery("select \t\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['nick_name'] nick_name,\n" +
                "\t`data`['head_img'] head_img,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['spu_id'] spu_id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['appraise'] appraise,\n" +
                "\t`data`['comment_txt'] comment_txt,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\tproc_time \n"+
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'comment_info'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("comment_info",comment_info);
    }


}
