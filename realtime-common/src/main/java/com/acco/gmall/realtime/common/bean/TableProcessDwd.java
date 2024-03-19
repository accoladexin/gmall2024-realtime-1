package com.acco.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcessDwd
 * Description: None
 * Package: com.acco.gmall.realtime.common.bean
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-07 12:54
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
