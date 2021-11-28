package cn.edu.sysu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author : song bei chang
 * @create 2021/8/2 19:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentInfo {

    private Long id;
    private Long order_id;
    private Long user_id;
    private BigDecimal total_amount;
    private String subject;
    private String payment_type;
    private String create_time;
    private String callback_time;

}



