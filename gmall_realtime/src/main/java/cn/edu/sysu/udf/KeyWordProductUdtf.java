package cn.edu.sysu.udf;

import cn.edu.sysu.bean.SystemConstant;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author : song bei chang
 * @create 2021/8/7 10:40
 */
@FunctionHint(output = @DataTypeHint("ROW<source string, ct bigint>"))
public class KeyWordProductUdtf extends TableFunction<Row> {


    public void eval(Long clickCount, Long cartCount, Long orderCount) {

        if(clickCount > 0){
            collect(Row.of(SystemConstant.KEYWORD_CLICK, clickCount));
        }

        if(cartCount > 0){
            collect(Row.of(SystemConstant.KEYWORD_CART, cartCount));
        }

        if(orderCount > 0){
            collect(Row.of(SystemConstant.KEYWORD_ORDER, orderCount));
        }
    }

}


