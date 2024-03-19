package com.acco.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ClassName: DorisMapFunction
 * Description: None
 * Package: com.acco.gmall.realtime.common.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-12 22:52
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig conf = new SerializeConfig();
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, conf);
    }
}
