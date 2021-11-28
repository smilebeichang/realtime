package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.ProvinceStats;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:38
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}


