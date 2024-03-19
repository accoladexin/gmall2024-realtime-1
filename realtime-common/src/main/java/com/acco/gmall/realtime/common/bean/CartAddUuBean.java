package com.acco.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ClassName: CartAddUuBean
 * Description: None
 * Package: com.acco.gmall.realtime.common.bean
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-15 14:35
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
