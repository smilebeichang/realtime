package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.ProvinceStats;
import cn.edu.sysu.gmall_publisher.mapper.ProvinceStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:39
 */
@Service
public class ProvinceStatsServiceImp implements ProvinceStatsService{
    @Autowired
    cn.edu.sysu.gmall_publisher.mapper.ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }

}



