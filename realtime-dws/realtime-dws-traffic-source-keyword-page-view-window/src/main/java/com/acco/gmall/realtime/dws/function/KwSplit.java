package com.acco.gmall.realtime.dws.function;

import com.acco.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ClassName: KwSplit
 * Description: None
 * Package: com.acco.gmall.realtime.dws.function
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-10 21:38
 */
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String kewwords) {
        Set<String> split = IkUtil.split(kewwords);
        for (String s : split) {
            collect(Row.of(s)); // 列明,看上面的@
        }


    }
}