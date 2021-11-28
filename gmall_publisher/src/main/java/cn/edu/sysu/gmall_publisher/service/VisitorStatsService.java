package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.VisitorStats;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:43
 */
public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHour(int date);

    Long getPv(int date);

    Long getUv(int date);
}



