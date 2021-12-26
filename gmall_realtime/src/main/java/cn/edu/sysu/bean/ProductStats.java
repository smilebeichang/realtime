package cn.edu.sysu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author : song bei chang
 * @create 2021/11/29 20:47
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {

    /**
     * 窗口起始时间
     */
    private String stt;

    /**
     * 窗口结束时间
     */
    private String edt;

    /**
     * sku编号
     */
    private Long sku_id;

    /**
     * sku名称
     */
    private String sku_name;

    /**
     * sku单价
     */
    private BigDecimal sku_price;

    /**
     * spu编号
     */
    private Long spu_id;

    /**
     * spu名称
     */
    private String spu_name;

    /**
     * 品牌编号
     */
    private Long tm_id;

    /**
     * 品牌名称
     */
    private String tm_name;

    /**
     * 品类编号
     */
    private Long category3_id;

    /**
     * 品类名称
     */
    private String category3_name;


    /**
     * 曝光数
     */
    private Long display_ct = 0L;

    /**
     * 点击数
     */
    private Long click_ct = 0L;

    /**
     * 收藏数
     */
    private Long favor_ct = 0L;

    /**
     * 添加购物车数
     */
    private Long cart_ct = 0L;

    /**
     * 下单商品个数
     */
    private Long order_sku_num = 0L;

    /**
     * 下单商品金额  不是整个订单的金额
     */
    private BigDecimal order_amount = BigDecimal.ZERO;

    /**
     * 订单数
     */
    private Long order_ct = 0L;

    /**
     * 支付金额
     */
    private BigDecimal payment_amount = BigDecimal.ZERO;

    /**
     * 支付订单数
     */
    private Long paid_order_ct = 0L;

    /**
     * 退款订单数
     */
    private Long refund_order_ct = 0L;

    private BigDecimal refund_amount = BigDecimal.ZERO;

    /**
     * 评论订单数
     */
    private Long comment_ct = 0L;

    /**
     * 好评订单数
     */
    private Long good_comment_ct = 0L;

    /**
     * 用于统计订单数
     */
    @NoSink
    private Set<Long> orderIdSet = new HashSet<>();

    /**
     * 用于统计支付订单数
     */
    @NoSink
    private Set<Long> paidOrderIdSet = new HashSet<>();

    /**
     * 用于退款支付订单数
     */
    @NoSink
    private Set<Long> refundOrderIdSet = new HashSet<>();

    /**
     * 统计时间戳
     */
    private Long ts;

}



