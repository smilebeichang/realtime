package cn.edu.sysu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/12/8 21:22
 */
public class TestRedis {


    public static void main(String[] args) {

        mapT();

    }

    public  static void jsonT(){
        Jedis client;
        client = MyRedisUtil.getClient();
        // 可以选择库，避免key过多，对其他业务产生影响
        client.select(1);

        String key = "dim_" + 1 + "_" + 2;

        // 1. 先从redis读取数据
        final String jsonStr = client.get(key);
        JSONObject obj;
        if (jsonStr != null) {
            // 方便调试
            System.out.println("走缓存..."+ key );
            // 1.1 缓存中有数据
            obj = JSON.parseObject(jsonStr);
            // 热点数据,过期时间延迟一个小时
            client.expire(key,24*60*60);
            System.out.println(jsonStr);

        } else {
            System.out.println("走数据库..." + key  );
            //1.2  缓存中没有数据, 需要从HBase读取数据

            // java.lang.IndexOutOfBoundsException: Index: 0, Size: 0 待优化,看来维度表数据不全啊
            obj = JSONObject.parseObject("{age:1}");
            // 1.3 数据写入缓存, 设置缓存失效时间7天 key,ddl,value
            client.setex(key, 3600 * 24 * 7, JSON.toJSONString(obj));
        }
    }
    public  static void mapT(){
        Jedis client;
        client = MyRedisUtil.getClient();
        // 可以选择库，避免key过多，对其他业务产生影响
        client.select(2);

        Map<String, String> map = new HashMap<>();
        map.put("1","ss1");
        map.put("2","ss2");
        map.put("3","ss3");
        map.put("4","ss4");
        map.put("4","ss5");

        client.hmset("fomfBase", map);

    }


}



