package cn.edu.sysu.gmall_publisher.mapper;

import cn.edu.sysu.gmall_publisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:38
 */
@Repository
public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select(
            "select " +
                    "   province_name," +
                    "   sum(order_amount) order_amount " +
                    "from province_stats_2021 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by province_id, province_name  ")
    List<ProvinceStats> selectProvinceStats(int date);
}


