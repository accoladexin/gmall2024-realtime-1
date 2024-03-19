package com.acco.gmall.realtime.dim.function;

import com.acco.gmall.realtime.common.bean.TableProcessDim;
import com.acco.gmall.realtime.common.util.JdbcUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

/**
 * ClassName: dimBroadcastFunction
 * Description: None
 * Package: com.acco.gmall.realtime.dim.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-25 22:38
 */
public class dimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    public HashMap<String,TableProcessDim> hashMap;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public dimBroadcastFunction(MapStateDescriptor mapDescriptor) {
        this.broadcastState = mapDescriptor;
    }


    // 初始化方法
    @Override
    public void open(Configuration parameters) throws Exception {
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, // 连接
                "select * from gmall2023_config.table_process_dim", // 查询语句，指定数据库
                TableProcessDim.class, // 查询结果转化的类型
                true);// 是下划线否转化驼峰
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            // System.out.println(tableProcessDim);
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    //处理广播数据 来一条处理一条
    @Override
    public void processBroadcastElement(TableProcessDim value,//广播流数据
                                        Context ctx, // 上下文
                                        Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        BroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // System.out.println(tableProcessState);
        String op = value.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(value.getSourceTable()); // 一条广播流数据
            // 处理hashmap数据
            hashMap.remove(value.getSourceTable());
        } else {
            // System.out.println(value.getSourceTable());
            tableProcessState.put(value.getSourceTable(), value); // 添加到广播状态
        }
    }
    // 处理主流数据 来一条处理一条
    @Override
    public void processElement(JSONObject value,
                               ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState1 = ctx.getBroadcastState(broadcastState);
        //查询广播状态
        String tableName = value.getString("table"); // 得到表的名字
        // System.out.println(tableName);

        TableProcessDim tableProcessDim = tableProcessState1.get(tableName); // 查询广播里面有没有相同的表名字

        // 预防维度表信息没有先过来
        if (tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }


        if (tableProcessDim != null) {
            // System.out.println(value);
            out.collect(Tuple2.of(value, tableProcessDim)); // 有 发送到下一条
        }

    }

}
