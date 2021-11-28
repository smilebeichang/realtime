package cn.edu.sysu.gmall_publisher.mapper;

import cn.edu.sysu.gmall_publisher.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:46
 */
@Repository
public interface KeywordStatsMapper {

    @Select(
            "select keyword," +
                    "   sum(keyword_stats_2021.ct * multiIf( " +
                    "       source='keyword_search',10, " +
                    "       source='ORDER',3, " +
                    "       source='CART',2, " +
                    "       source='CLICK',1, " +
                    "       0 " +
                    "   )) ct " +
                    "from keyword_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by keyword " +
                    "order by sum(keyword_stats_2021.ct) desc " +
                    "limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date,
                                                 @Param("limit") int limit);
}



