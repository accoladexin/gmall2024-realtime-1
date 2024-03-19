package com.acco.gmall.realtime.dim.function;

import com.acco.gmall.realtime.common.bean.TableProcessDim;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.HBaseUtil;
import com.acco.gmall.realtime.common.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import redis.clients.jedis.Jedis;

import java.awt.geom.RectangularShape;
import java.io.IOException;

/**
 * ClassName: DimHBaseSinkFunction
 * Description: None
 * Package: com.acco.gmall.realtime.dim.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-02-26 16:07
 */
public class DimHBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;
    Jedis jedis; // 主要是考虑redis中的数据更新


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }
    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject JsonObj = value.f0;
        TableProcessDim dim = value.f1;
        String type = JsonObj.getString("type");
        JSONObject data = JsonObj.getJSONObject("data");
        if ("delete".equals(type)) { // 删除数据
            delete(data, dim);
        } else { //覆盖写入
            put(data, dim);
        }
        // 判断redis中的数据是否发生了变化
        if("delete".equals(type)|| "update".equals(type)){ // 跟新和删除都要在redis里面删除
            jedis.del(RedisUtil.getRedisKey(dim.getSinkTable(),data.getString(dim.getSinkRowKey())));
        }



    }
    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKey = dim.getSinkRowKey();//主键
        try {
            HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE,sinkTable,sinkRowKey);
        } catch (IOException e) {
            System.out.println("删除失败:"+Constant.HBASE_NAMESPACE+"."+sinkTable+": "+sinkRowKey);
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKey = dim.getSinkRowKey();//主键
        String sinkRowValue = data.getString(sinkRowKey);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection,Constant.HBASE_NAMESPACE,sinkTable,sinkRowValue,sinkFamily,data);
        } catch (IOException e) {
            System.out.println("更新失败:"+Constant.HBASE_NAMESPACE+"."+sinkTable+": "+sinkRowKey);
        }
    }

}
