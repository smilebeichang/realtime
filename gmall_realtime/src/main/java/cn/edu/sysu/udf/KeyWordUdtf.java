package cn.edu.sysu.udf;

import cn.edu.sysu.util.KeyWordUtil;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/7 8:44
 */
// 表示输出数据的类型
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeyWordUdtf extends TableFunction<Row> {

    // 可以根据需要实现多个重置的eval方法
    public void eval(String str) {
        final List<String> words = KeyWordUtil.analyze(str);
        for (String word : words) {
            // 一进多出
            collect(Row.of(word));
        }
    }


}



