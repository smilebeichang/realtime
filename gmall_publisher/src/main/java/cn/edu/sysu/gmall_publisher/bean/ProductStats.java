package cn.edu.sysu.gmall_publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:32
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {

    private String stt;//窗口起始时间
    private String edt;  //窗口结束时间
    private Long sku_id; //sku编号
    private String sku_name;//sku名称
    private BigDecimal sku_price; //sku单价
    private Long spu_id; //spu编号
    private String spu_name;//spu名称
    private Long tm_id; //品牌编号
    private String tm_name;//品牌名称
    private Long category3_id;//品类编号
    private String category3_name;//品类名称

    private Long display_ct = 0L; //曝光数

    private Long click_ct = 0L;  //点击数

    private Long favor_ct = 0L; //收藏数

    private Long cart_ct = 0L;  //添加购物车数

    private Long order_sku_num = 0L; //下单商品个数

    //下单商品金额  不是整个订单的金额
    private BigDecimal order_amount = BigDecimal.ZERO;

    private Long order_ct = 0L; //订单数

    //支付金额
    private BigDecimal payment_amount = BigDecimal.ZERO;

    private Long paid_order_ct = 0L;  //支付订单数

    private Long refund_order_ct = 0L; //退款订单数

    private BigDecimal refund_amount = BigDecimal.ZERO;

    private Long comment_ct = 0L;//评论订单数

    private Long good_comment_ct = 0L; //好评订单数

    transient private Set<Long> orderIdSet = new HashSet<>();  //用于统计订单数

    transient private Set<Long> paidOrderIdSet = new HashSet<>(); //用于统计支付订单数

    transient private Set<Long> refundOrderIdSet = new HashSet<>();//用于统计退款订单数

    private Long ts; //统计时间戳

}



