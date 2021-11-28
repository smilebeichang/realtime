package cn.edu.sysu.gmall_publisher.service;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:20
 */
import cn.edu.sysu.gmall_publisher.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {

    /**
     * 总成交金额
     */
    BigDecimal getGMV(int date);


    /**
     * 统计某天不同品牌商品交易额排名
     */
    List<ProductStats> getProductStatsByTrademark(int date, int limit);


    /**
     * 统计某天不同类别商品交易额排名
     */
    List<ProductStats> getProductStatsGroupByCategory3(int date, int limit);


    /**
     *  统计某天不同SPU商品交易额排名
     */
    List<ProductStats> getProductStatsGroupBySpu(int date, int limit);


}
