package com.acco.gmall.realtime.dwd.db.app;

import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderDetail
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 12:37
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10015,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        // TODO 设置TTL 否则上线就爆内存
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L)); //5秒的ttl
        // TODO 从topicdb里面读取数据 筛选
//        createTopicDbkafka(ckAndGroupId,tEnv);
        tEnv.executeSql("select `data` from topic_db").print();
        // 筛选订单表详情数据
        Table odtable = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "    ts\n" +
                "from \n" +
                "topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail",odtable);
        // 筛选订单表字段
        Table oitable = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['province_id'] province_id\n" +
                "from \n" +
                "    topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_info",oitable);
        // 筛选订单活动表格
        Table oditable = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id\n" +
                "from \n" +
                "    topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tEnv.createTemporaryView("order_detail_activity",oditable);
        // 筛选活动优惠卷相关表格

        Table odctable = tEnv.sqlQuery("select \n" +
                "    `data`['order_detail_id'] order_detail_id,\n" +
                "    `data`['coupon_id'] coupon_id\n" +
                "from \n" +
                "    topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_detail_coupon",odctable);

        /**tEnv.executeSql("select\n" +
                "    od.id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    user_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    create_time,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "    on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "    on oda.order_detail_id = od.id\n" +
                "left join  order_detail_coupon odc\n" +
                "    on odc.coupon_id = od.id").print(); */

        // TODO 关联四张表格
        Table jointable = tEnv.sqlQuery("select\n" +
                "    od.id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    user_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    create_time,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "    on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "    on oda.order_detail_id = od.id\n" +
                "left join  order_detail_coupon odc\n" +
                "    on odc.coupon_id = od.id");
        // TODO 创建kafka对应的表格
        // 使用了left join有测回流,需要使用upsert kafka
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+" (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    user_id STRING,\n" +
                "    province_id STRING,\n" +
                "    activity_id STRING,\n" +
                "    activity_rule_id STRING,\n" +
                "    coupon_id STRING,\n" +
                "    sku_name STRING,\n" +
                "    order_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    create_time STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") "+ SQLUtil.getUpsertKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        // TODO 写到kafka表格中
        jointable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();


    }
}
