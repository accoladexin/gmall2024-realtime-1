package com.acco.gmall.realtime.common.function;

import com.acco.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: DimJoinFunction
 * Description: None
 * Package: com.acco.gmall.realtime.common.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-17 21:43
 */
public interface DimJoinFunction<T> {
    public abstract String getId(T bean);
    public abstract String getTableName();
    public  abstract void join(T input, JSONObject dim);
}
