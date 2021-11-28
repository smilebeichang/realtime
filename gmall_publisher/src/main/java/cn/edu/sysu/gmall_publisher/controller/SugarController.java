package cn.edu.sysu.gmall_publisher.controller;

import cn.edu.sysu.gmall_publisher.bean.KeywordStats;
import cn.edu.sysu.gmall_publisher.bean.ProductStats;
import cn.edu.sysu.gmall_publisher.bean.ProvinceStats;
import cn.edu.sysu.gmall_publisher.bean.VisitorStats;
import cn.edu.sysu.gmall_publisher.service.KeywordStatsService;
import cn.edu.sysu.gmall_publisher.service.ProductStatsService;
import cn.edu.sysu.gmall_publisher.service.ProvinceStatsService;
import cn.edu.sysu.gmall_publisher.service.VisitorStatsService;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author : song bei chang
 * @create 2021/8/8 16:22
 *
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    VisitorStatsService visitorStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;


    @RequestMapping("/gmv")
    public String gmv(@RequestParam(value = "date", defaultValue = "0") int date) {
        // 如果没有传时间, 就使用今天
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }

        final BigDecimal gmv = productStatsService.getGMV(date);
        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);
        return JSON.toJSONString(result);

    }


    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                             @RequestParam(value = "limit", defaultValue = "20") int limit) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        List<ProductStats> productStatsByTrademarkList
                = productStatsService.getProductStatsByTrademark(date, limit);
        List<String> tradeMarkList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();
        for (ProductStats productStats : productStatsByTrademarkList) {
            tradeMarkList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }

        final HashMap<String, Object> result = new HashMap<>();

        result.put("status", 0);
        result.put("msg", "");

        final HashMap<String, Object> data = new HashMap<>();

        data.put("categories", tradeMarkList);

        final ArrayList<Map<String, Object>> series = new ArrayList<>();
        final HashMap<String, Object> one = new HashMap<>();
        one.put("name", "商品品牌");
        one.put("data", amountList);
        series.add(one);

        data.put("series", series);
        result.put("data", data);
        return JSON.toJSONString(result);

    }



    @RequestMapping("/category3")
    public String getProductStatsGroupByCategory3(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                                  @RequestParam(value = "limit", defaultValue = "4") int limit) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        List<ProductStats> statsList
                = productStatsService.getProductStatsGroupByCategory3(date, limit);

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        final List<Map<String, Object>> data = new ArrayList<>();
        for (ProductStats productStats : statsList) {
            final HashMap<String, Object> map = new HashMap<>();
            map.put("name", productStats.getCategory3_name());
            map.put("value", productStats.getOrder_amount());
            data.add(map);
        }
        result.put("data", data);

        return JSON.toJSONString(result);
    }


    @RequestMapping("/spu")
    public String getProductStatsGroupBySpu(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        System.out.println(date);
        System.out.println(limit);
        List<ProductStats> statsList
                = productStatsService.getProductStatsGroupBySpu(date, limit);

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        final HashMap<String, Object> data = new HashMap<>();

        final ArrayList<Map<String, Object>> columns = new ArrayList<>();
        final HashMap<String, Object> c1 = new HashMap<>();
        c1.put("name", "商品名次");
        c1.put("id", "spu_name");
        columns.add(c1);
        final HashMap<String, Object> c2 = new HashMap<>();
        c2.put("name", "交易额");
        c2.put("id", "order_amount");
        columns.add(c2);
        final HashMap<String, Object> c3 = new HashMap<>();
        c3.put("name", "订单数");
        c3.put("id", "order_ct");
        columns.add(c3);
        data.put("columns", columns);

        final ArrayList<Map<String, Object>> rows = new ArrayList<>();
        for (ProductStats productStats : statsList) {
            final HashMap<String, Object> row = new HashMap<>();
            row.put("spu_name", productStats.getSpu_name());
            row.put("order_amount", productStats.getOrder_amount());
            row.put("order_ct", productStats.getOrder_ct());
            rows.add(row);
        }
        data.put("rows", rows);

        result.put("data", data);
        return JSON.toJSONString(result);
    }




    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        final List<ProvinceStats> provinceStats = provinceStatsService.getProvinceStats(date);

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        final HashMap<String, Object> data = new HashMap<>();

        final ArrayList<Map<String, Object>> mapData = new ArrayList<>();
        for (ProvinceStats provinceStat : provinceStats) {
            final HashMap<String, Object> map = new HashMap<>();
            map.put("name", provinceStat.getProvince_name());
            map.put("value", provinceStat.getOrder_amount());
            mapData.add(map);
        }

        data.put("mapData", mapData);
        data.put("valueName", "交易额");
        result.put("data", data);

        return JSON.toJSONString(result);

    }






    @RequestMapping("/hr")
    public String getMidStatsGroupByHourNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        List<VisitorStats> visitorStatsHrList
                = visitorStatsService.getVisitorStatsByHour(date);

        //构建24位数组
        VisitorStats[] visitorStatsArr = new VisitorStats[24];

        //把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorStatsHrList) {
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }
        List<String> hrList = new ArrayList<>();
        List<Long> uvList = new ArrayList<>();
        List<Long> pvList = new ArrayList<>();
        List<Long> newMidList = new ArrayList<>();

        //循环出固定的0-23个小时  从结果map中查询对应的值
        for (int hr = 0; hr <= 23; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats != null) {
                uvList.add(visitorStats.getUv_ct());
                pvList.add(visitorStats.getPv_ct());
                newMidList.add(visitorStats.getNew_uv());
            } else { //该小时没有流量补零
                uvList.add(0L);
                pvList.add(0L);
                newMidList.add(0L);
            }
            //小时数不足两位补零
            hrList.add(String.format("%02d", hr));
        }

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        final HashMap<String, Object> data = new HashMap<>();

        data.put("categories", hrList);

        final ArrayList<Map<String, Object>> series = new ArrayList<>();
        final HashMap<String, Object> uv = new HashMap<>();
        uv.put("name", "uv");
        uv.put("data", uvList);
        series.add(uv);
        final HashMap<String, Object> pv = new HashMap<>();
        pv.put("name", "pv");
        pv.put("data", pvList);
        series.add(pv);
        final HashMap<String, Object> newUsers = new HashMap<>();
        newUsers.put("name", "新用户");
        newUsers.put("data", newMidList);
        series.add(newUsers);

        data.put("series", series);
        result.put("data", data);

        return JSON.toJSONString(result);
    }


    @RequestMapping("/visitor")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }

        List<VisitorStats> visitorStatsByNewFlag = visitorStatsService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        //循环把数据赋给新访客统计对象和老访客统计对象
        for (VisitorStats visitorStats : visitorStatsByNewFlag) {
            if (visitorStats.getIs_new().equals("1")) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }

        // 准备返回json字符串

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        final HashMap<String, Object> data = new HashMap<>();

        // 表头
        final ArrayList<Map<String, Object>> columns = new ArrayList<>();
        final HashMap<String, Object> c1 = new HashMap<>();
        c1.put("name", "类别");
        c1.put("id", "type");
        columns.add(c1);
        final HashMap<String, Object> c2 = new HashMap<>();
        c2.put("name", "新用户");
        c2.put("id", "new");
        columns.add(c2);
        final HashMap<String, Object> c3 = new HashMap<>();
        c3.put("name", "旧用户");
        c3.put("id", "old");
        columns.add(c3);
        data.put("columns", columns);

        // 表内容
        final ArrayList<Map<String, Object>> rows = new ArrayList<>();
        final HashMap<String, Object> row1 = new HashMap<>();
        row1.put("type", "用户数(人)");
        row1.put("new", newVisitorStats.getUv_ct());
        row1.put("old", oldVisitorStats.getUv_ct());
        rows.add(row1);
        final HashMap<String, Object> row2 = new HashMap<>();
        row2.put("type", "总访问页面(次)");
        row2.put("new", newVisitorStats.getPv_ct());
        row2.put("old", oldVisitorStats.getPv_ct());
        rows.add(row2);
        final HashMap<String, Object> row3 = new HashMap<>();
        row3.put("type", "跳出率(%)");
        row3.put("new", newVisitorStats.getUjRate());
        row3.put("old", oldVisitorStats.getUjRate());
        rows.add(row3);
        final HashMap<String, Object> row4 = new HashMap<>();
        row4.put("type", "平均在线时长(s)");
        row4.put("new", newVisitorStats.getDurPerSv());
        row4.put("old", oldVisitorStats.getDurPerSv());
        rows.add(row4);
        final HashMap<String, Object> row5 = new HashMap<>();
        row5.put("type", "平均访问页面数(人次)");
        row5.put("new", newVisitorStats.getPvPerSv());
        row5.put("old", oldVisitorStats.getPvPerSv());
        rows.add(row5);

        data.put("rows", rows);

        result.put("data", data);
        System.out.println(JSON.toJSONString(result));
        return JSON.toJSONString(result);
    }




    @Autowired
    KeywordStatsService keywordStatsService;

    @RequestMapping("/keyword")
    public String getKeywordStats(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit", defaultValue = "20") int limit) {

        if (date == 0) {
            date = Integer.parseInt(DateFormatUtils.format(new Date(), "yyyyMMDD"));
        }
        //查询数据
        List<KeywordStats> keywordStatsList
                = keywordStatsService.getKeywordStats(date, limit);

        final HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");
        final ArrayList<Map<String, Object>> data = new ArrayList<>();
        for (KeywordStats keywordStats : keywordStatsList) {
            final HashMap<String, Object> map = new HashMap<>();
            map.put("name", keywordStats.getKeyword());
            map.put("value", keywordStats.getCt());
            data.add(map);
        }
        result.put("data", data);

        return JSON.toJSONString(result);
    }




}



