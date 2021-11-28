package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.ProductStats;
import cn.edu.sysu.gmall_publisher.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:21
 */
@Service
public class ProductStatsServiceImp implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }



    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        return productStatsMapper.getProductStatsByTrademark(date, limit);
    }


    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date, limit);
    }


    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date, limit);
    }


}


