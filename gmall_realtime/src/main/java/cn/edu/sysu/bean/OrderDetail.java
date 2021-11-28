package cn.edu.sysu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author : song bei chang
 * @create 2021/11/28 16:29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetail {

    private Long id;
    private Long order_id;
    private Long sku_id;
    private BigDecimal order_price;
    private Long sku_num;
    private String sku_name;
    private String create_time;
    private BigDecimal split_total_amount;
    private BigDecimal split_activity_amount;
    private BigDecimal split_coupon_amount;
    private Long create_ts;

    /** 为了create_ts 时间戳赋值, 所以需要手动补充 */
    public void setCreate_time(String create_time) throws ParseException {
        this.create_time = create_time;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.create_ts = sdf.parse(create_time).getTime();

    }

}


