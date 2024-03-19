package com.acco.gmall.realtime.dwd.db.app;

import com.acco.gmall.realtime.common.base.BaseSQLApp;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderPaySucDetail
 * Description: None
 * Package: com.acco.gmall.realtime.dwd.db.app
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-06 21:11
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        // 1.读取topic_db的数据
        createTopicDbkafka(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,tEnv);
//        tEnv.executeSql("select * from topic_db").print();

        // 筛选出支付成功的数据
        Table paymenttable = tEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "    ts,\n" +
                "    order_time, \n" +
                "    proc_time \n" +
                "from \n" +
                "    topic_db\n" +
                "where `database`= 'gmall'\n" +
                "and `table` = 'payment_info'\n" +
                "and `type` = 'update' \n" +
                "and `old`['payment_status'] is not null\n" +
                "and `data`['payment_status'] = '1602'");
        tEnv.createTemporaryView("payment",paymenttable);

        // 读取下单表详情字段
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
                "   order_time as TO_TIMESTAMP_LTZ(ts, 3),\n" + // 事件事件 innter join一定要事件事件
                "WATERMARK FOR order_time AS order_time - INTERVAL '15' SECOND\n" + //水位线
                ") "+ SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,ckAndGroupId));
        // 获取base_dic字典表
        createBaseDic(tEnv);
//        tEnv.executeSql("select * from base_dic").print();

        // 使用inner join 连接支付表和支付详情表,innerjoin要用事件事件
        Table payOrdertable = tEnv.sqlQuery("SELECT \n" +
                "    od.id ,\n" +
                "    p.order_id ,\n" +
                "    p.user_id ,\n" +
                "    payment_type ,\n" +
                "    callback_time payment_time,\n" +
                "    sku_id ,\n" +
                "    province_id ,\n" +
                "    activity_id ,\n" +
                "    activity_rule_id ,\n" +
                "    coupon_id ,\n" +
                "    sku_name ,\n" +
                "    order_price ,\n" +
                "    sku_num ,\n" +
                "    split_total_amount ,\n" +
                "    split_activity_amount ,\n" +
                "    split_coupon_amount ,\n" +
                "    p.ts,    \n" +
                        "p.proc_time \n" +
                "FROM payment p, " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " od\n" +
                "WHERE p.order_id = od.order_id\n" +
                "AND p.order_time BETWEEN od.order_time - INTERVAL '15' MINUTE AND od.order_time + INTERVAL '5' SECOND");
        tEnv.createTemporaryView("pay_order",payOrdertable);

        // 使用lookup join 实现维度退化 要用事件事件
        Table lookuptable = tEnv.sqlQuery("SELECT \n" +
                "    id ,\n" +
                "    order_id ,\n" +
                "    user_id ,\n" +
                "    payment_type payment_type_code,\n" +
                "    info.dic_name payment_type_name,\n" +
                "    payment_time,\n" +
                "    sku_id ,\n" +
                "    province_id ,\n" +
                "    activity_id ,\n" +
                "    activity_rule_id ,\n" +
                "    coupon_id ,\n" +
                "    sku_name ,\n" +
                "    order_price ,\n" +
                "    sku_num ,\n" +
                "    split_total_amount ,\n" +
                "    split_activity_amount ,\n" +
                "    split_coupon_amount ,\n" +
                "    ts\n" +
                "FROM pay_order p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b\n" +
                "on p.payment_type = b.dic_code");
        // 创建upsert kafka 表格
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+" (\n" +
                "    id STRING ,\n" +
                "    order_id STRING ,\n" +
                "    user_id STRING ,\n" +
                "    payment_type_code STRING,\n" +
                "    payment_type_name STRING,\n" +
                "    payment_time STRING,\n" +
                "    sku_id STRING ,\n" +
                "    province_id STRING ,\n" +
                "    activity_id STRING ,\n" +
                "    activity_rule_id STRING ,\n" +
                "    coupon_id STRING ,\n" +
                "    sku_name STRING ,\n" +
                "    order_price STRING ,\n" +
                "    sku_num STRING ,\n" +
                "    split_total_amount STRING ,\n" +
                "    split_activity_amount STRING ,\n" +
                "    split_coupon_amount STRING ,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        lookuptable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();


    }
}
