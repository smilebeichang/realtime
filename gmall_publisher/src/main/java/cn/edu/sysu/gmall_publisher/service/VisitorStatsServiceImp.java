package cn.edu.sysu.gmall_publisher.service;

import cn.edu.sysu.gmall_publisher.bean.VisitorStats;
import cn.edu.sysu.gmall_publisher.mapper.VisitorStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:43
 */
@Service
public class VisitorStatsServiceImp implements VisitorStatsService{

    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(int date) {
        return visitorStatsMapper.selectPv(date);
    }

    @Override
    public Long getUv(int date) {
        return visitorStatsMapper.selectUv(date);
    }

}


