package com.acco.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * ClassName: TradeOrderBean
 * Description: None
 * Package: com.acco.gmall.realtime.common.bean
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-15 14:53
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    // 当天日期
    String curDate;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
