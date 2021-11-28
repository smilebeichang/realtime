package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.KeywordStats;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:47
 */
public interface KeywordStatsService {
    List<KeywordStats> getKeywordStats(int date,
                                       int limit);
}


