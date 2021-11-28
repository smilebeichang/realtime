package cn.edu.sysu.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author : song bei chang
 * @create 2021/7/31 16:03
 */
public class MyRedisUtil {
    public static JedisPool jedisPool = null;

    public static Jedis getClient() {

        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //最大可用连接数
            jedisPoolConfig.setMaxTotal(100);
            //连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            //等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);

            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(10);
            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);

            //取连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "ecs2", 6379, 5000);
            System.out.println("开辟连接池");

        } else {
            System.out.println(" 连接池:" + jedisPool.getNumActive());
        }
        return jedisPool.getResource();
    }
}


