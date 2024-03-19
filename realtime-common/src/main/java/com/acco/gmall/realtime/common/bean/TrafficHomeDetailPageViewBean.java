package com.acco.gmall.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: TrafficHomeDetailPageViewBean
 * Description: None
 * Package: com.acco.gmall.realtime.common.bean
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-12 23:08
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
