package com.acco.gmall.realtime.dwd.db.app;

import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.xml.crypto.Data;

/**
 * ClassName: DwdTradeCartAdd
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 11:44
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        // TODO 从topic_DB读取数据

        // TODo
        createTopicDbkafka(ckAndGroupId,tEnv);
//        tEnv.executeSql("select * from topic_db").print();
        // TODO 筛选出加购的数据
        Table cartAdd = getCartAdd(tEnv);

        // TODO 创建写入到kafka的表格
        // 3. 写出到 kafka
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+" (" +
                "   id string, " +
                "   user_id string," +
                "   sku_id string," +
                "   sku_num int, " +
                "   ts  bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }








    private Table getCartAdd(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                " `data`['id'] id," +
                " `data`['user_id'] user_id," +
                " `data`['sku_id'] sku_id," +
                " if(`type`='insert'," +
                "   cast(`data`['sku_num'] as int), " +
                "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                ") sku_num ," +
                " ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='cart_info' " +
                "and (" +
                " `type`='insert' " +
                "  or(" +
                "     `type`='update' " +
                "      and `old`['sku_num'] is not null " +
                "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                "   )" +
                ")");
    }

}
