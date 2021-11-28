package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.KeywordStats;
import cn.edu.sysu.gmall_publisher.mapper.KeywordStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:47
 */
@Service
public class KeywordStatsServiceImp implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date, limit);
    }
}


