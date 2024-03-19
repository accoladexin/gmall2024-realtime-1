package com.acco.gmall.realtime.common.function;

import com.acco.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.acco.gmall.realtime.common.constant.Constant;
import com.acco.gmall.realtime.common.util.HBaseUtil;
import com.acco.gmall.realtime.common.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ClassName: AsyncDimFunction
 * Description: None
 * Package: com.acco.gmall.realtime.common.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-17 21:00
 */


/**
 *
 * @param <T> 数据流的类型
 */
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    String tableName = getTableName();

    boolean flag = false; // 只用于输出信息的


    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);

    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // java的异步编程方式
        String rowKey = getId(input);
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 异步获得的数据
                // 异步获取数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() { //再来一次异步访问
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject jsonObject = null;
                if (dimInfo == null || dimInfo.length() == 0) {
                    // redis没有,需要在hbase里面去找
                    if (flag) System.out.println("redis无数据:" + tableName + ":" + rowKey);
                    try {
                        jsonObject = HBaseUtil.readDimAsync(
                                hBaseAsyncConnection,
                                Constant.HBASE_NAMESPACE,
                                tableName,
                                rowKey
                        );
                        // 读取到的数据保存到redis中
                        redisAsyncConnection.async().setex(redisKey, 24 * 60 * 60, jsonObject.toJSONString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    // redis中有值
                    if (flag) System.out.println("redis有数据:" + tableName + ":" + rowKey);
                    jsonObject = JSONObject.parseObject(dimInfo);

                }
                return jsonObject;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dimSkuInfo) {
                // 读到的值
                //合并维度信息
                if (dimSkuInfo == null || dimSkuInfo.size() == 0) {
                    // 无法关联到信息

                    if (flag)System.out.println("无法关联到信息:" + tableName + ":" + rowKey);
                } else {
                    // 维度关联
                    join(input,dimSkuInfo);
                }
                //返回结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });

    }




}
