package com.acco.gmall.realtime.common.constant;

/**
 * ClassName: Constant
 * Description: None
 * Package: com.acco.gmall.realtime.common.constant
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-23 13:53
 */
public class Constant {
    public static  final  String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";


    public static final String PROCESS_DATABASE = "gmall2023_config";
    public static final String PROCESS_DIM_TABLE_NAME = "table_process_dim";
    public static final String PROCESS_DWM_TABLE_NAME = "table_process_dwd";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop102,hadoop103,hadoop104";





    public static final String HBASE_NAMESPACE = "gmall";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_2024";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_2024";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_2024";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_2024";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_2024";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info_2024";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add_2024";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail_2024";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_2024";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success_2024";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund_2024";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success_2024";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String FENODES = "hadoop102:7030";
    public static final String DORIS_DATABAES = "gmall2023_realtime";
    public static final String DORIS_NAME = "root";
    public static final String DORISL_PASSWORD = "aaaaaa";

    public static final String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW= "dws_traffic_home_detail_page_view_window";
    public static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW ="dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_USER_USER_LOGIN_WINDOW ="dws_user_user_login_window";

    public static final String DWS_USER_USER_REGISTER_WINDOW = "dws_user_user_register_window";

    public static String DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";

    public  static  String DWS_TRADE_PAYMENT_SUC_WINDOW = "dws_trade_payment_suc_window";

    public static  String DWS_TRADE_ORDER_WINDOW = "dws_trade_order_window";
    public static  int TWO_DAY_SECONDS = 2;
    public static  String DWS_TRADE_SKU_ORDER_WINDOW= "dws_trade_sku_order_window";

}