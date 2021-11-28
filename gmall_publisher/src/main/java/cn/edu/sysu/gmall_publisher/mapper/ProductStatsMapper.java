package cn.edu.sysu.gmall_publisher.mapper;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:19
 */

import cn.edu.sysu.gmall_publisher.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品统计
 */
@Repository
public interface ProductStatsMapper {
    /**
     *  toYYYYMMDD(stt)是把日志转成数字的函数(ClickHouse) 比如:2021-03-01 -> 20210301
     */
    @Select(
            "select " +
                    "   sum(order_amount) order_amount " +
                    "from product_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);


    /**
     * 统计某天不同品牌商品交易额排名
     */
    @Select(
            "select " +
                    "   tm_id," +
                    "   tm_name, " +
                    "   sum(order_amount) order_amount " +
                    "from product_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by tm_id,tm_name " +
                    "having order_amount>0  " +
                    "order by  order_amount  desc " +
                    "limit #{limit} ")
    List<ProductStats> getProductStatsByTrademark(@Param("date") int date,
                                                         @Param("limit") int limit);

    /**
     * 统计某天不同类别商品交易额排名
     */
    @Select(
            "select " +
                    "   category3_id," +
                    "   category3_name," +
                    "   sum(order_amount) order_amount " +
                    "   from product_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by category3_id,category3_name " +
                    "having order_amount>0  " +
                    "order by  order_amount desc " +
                    "limit #{limit}")
    List<ProductStats> getProductStatsGroupByCategory3(@Param("date") int date,
                                                              @Param("limit") int limit);

    /**
     * 统计某天不同SPU商品交易额排名
     */
    @Select(
            "select " +
                    "   spu_id," +
                    "   spu_name," +
                    "   sum(order_amount) order_amount," +
                    "   sum(order_ct) order_ct " +
                    "from product_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by spu_id,spu_name " +
                    "having order_amount>0 " +
                    "order by order_amount desc " +
                    "limit #{limit} ")
    List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date,
                                                        @Param("limit") int limit);

}
