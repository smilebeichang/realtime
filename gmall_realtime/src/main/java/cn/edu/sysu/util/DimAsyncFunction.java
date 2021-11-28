package cn.edu.sysu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author : song bei chang
 * @create 2021/8/6 10:17
 *
 *      旁路缓存 + 异步IO
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    String phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    private Connection conn;
    private ThreadPoolExecutor pool;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = MyJDBCUtil.getConnection(phoenixUrl);
        // 获取一个线程池
        pool = MyThreadPoolUtil.getInstance();
    }

    @Override
    public void close() throws Exception {
        // 关闭线程池
        pool.shutdownNow();
        conn.close();
    }

    public JSONObject readDim(Jedis client, String table, String id) {
        String sql = "select * from " + table + " where id=?";
        String key = "dim_" + table + "_" + id;
        // 1. 先从redis读取数据
        final String jsonStr = client.get(key);
        JSONObject obj;
        if (jsonStr != null) {
            System.out.println("走缓存...");
            // 1.1 缓存中有数据
            obj = JSON.parseObject(jsonStr);
        } else {
            System.out.println("走数据库...");
            //1.2  缓存中没有数据, 需要从HBase读取数据
            final List<JSONObject> dim = MyJDBCUtil
                    .queryList(conn, sql, new Object[]{id}, JSONObject.class, false);
            System.out.println(sql + "   " + id);
            obj = dim.get(0);
            // 1.3 数据写入缓存, 设置缓存失效时间7天
            client.setex(key, 3600 * 24 * 7, JSON.toJSONString(obj));
        }
        return obj;
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        pool.execute(new Runnable() {
            @Override
            public void run() {
                final Jedis client = MyRedisUtil.getClient();
                addDimToInput(input, client);
                client.close();
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
    // 子类在此方法中实现如何把读取的维度信息添加到输入数据中
    protected abstract void addDimToInput(T input, Jedis client);

}



