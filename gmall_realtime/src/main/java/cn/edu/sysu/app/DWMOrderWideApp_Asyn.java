package cn.edu.sysu.app;


import cn.edu.sysu.bean.OrderDetail;
import cn.edu.sysu.bean.OrderInfo;
import cn.edu.sysu.bean.OrderWide;
import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.MyJDBCUtil;
import cn.edu.sysu.util.MyKafkaUtil;
import cn.edu.sysu.util.MyRedisUtil;
import cn.edu.sysu.util.MyThreadPoolUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author : song bei chang
 * @create 2021/7/31 7:39
 *
 *  双流 intervalJoin
 */
public class DWMOrderWideApp_Asyn extends BaseAppV2 {

    public static void main(String[] args) {

        new DWMOrderWideApp_Asyn().init(1,
                "DWMOrderWideApp_Asyn",
                "dwd_order_info",
                "dwd_order_detail");

    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {

        setWebUi(env, 8888);
        // 1. 订单表和订单明细表join
        final SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim = factJoin(sourceStreams);

        //orderWideStreamWithoutDim.print("join")

        // 2. join维度信息
        final SingleOutputStreamOperator<OrderWide> orderWideStreamWithDim = readDim(orderWideStreamWithoutDim);

        //orderWideStreamWithDim.print("dim")

        // 3. 写入kafka
        saveToKafka(orderWideStreamWithDim);

    }

    /**
     * map 转换为 JSONString
     *
     */
    private void saveToKafka(SingleOutputStreamOperator<OrderWide> orderWideStreamWithDim) {

        orderWideStreamWithDim
                //.map(map -> map.toString())
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWM_ORDER_WIDE));

    }


    /**
     * join 维度数据
     *
     * @param orderWideStreamWithoutDim 没有维度信息的流
     * @return SingleOutputStreamOperator<OrderWide> join维度表之后的流
     */
    private SingleOutputStreamOperator<OrderWide> readDim(SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim) {

        String phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";

        // AsyncDataStream 流的异步处理
        final SingleOutputStreamOperator<OrderWide> OrderWideWithUserInfo = AsyncDataStream.unorderedWait(
                orderWideStreamWithoutDim,
                // 匿名内部类  RichAsyncFunction
                new RichAsyncFunction<OrderWide, OrderWide>() {

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

                    private JSONObject readDim(Jedis client, String table, String id) {
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
                            obj = dim.get(0);
                            // 1.3 数据写入缓存, 设置缓存失效时间7天
                            client.setex(key, 3600 * 24 * 7, JSON.toJSONString(obj));
                        }
                        return obj;
                    }

                    // 异步调用函数
                    @Override
                    public void asyncInvoke(OrderWide orderWide,
                                            ResultFuture<OrderWide> resultFuture) throws Exception {
                        pool.execute(new Runnable() {
                            @Override
                            public void run() {
                                // 在子线程中去读取维度数据,
                                // 为了简单起见, 在一个异步操作中连接6张维度表
                                // 也可以一个操作连接1张维度表, 则需要6个异步操作
                                // alt + r  重构字段名，全部替换，智能
                                final Jedis client = MyRedisUtil.getClient();

                                // 1. join user_info
                                final JSONObject userInfoObj = readDim(client, "dim_user_info", orderWide.getUser_id().toString());
                                orderWide.setUser_gender(userInfoObj.getString("GENDER"));
                                orderWide.calcUserAgeByBirthday(userInfoObj.getString("BIRTHDAY"));

                                // 2. join 省份维度
                                final JSONObject provinceObj = readDim(client, "dim_base_province", orderWide.getProvince_id().toString());
                                orderWide.setProvince_3166_2_code(provinceObj.getString("ISO_3166_2"));
                                orderWide.setProvince_area_code(provinceObj.getString("AREA_CODE"));
                                orderWide.setProvince_name(provinceObj.getString("NAME"));
                                orderWide.setProvince_iso_code(provinceObj.getString("ISO_CODE"));

                                // 3. join sku维度
                                final JSONObject skuObj = readDim(client, "dim_sku_info", orderWide.getSku_id() + "");
                                orderWide.setSku_name(skuObj.getString("SKU_NAME"));
                                orderWide.setSpu_id(skuObj.getLong("SPU_ID"));
                                orderWide.setCategory3_id(skuObj.getLong("CATEGORY3_ID"));
                                orderWide.setTm_id(skuObj.getLong("TM_ID"));

                                // 4. join spu维度
                                final JSONObject spuObj = readDim(client, "dim_spu_info", orderWide.getSpu_id() + "");
                                orderWide.setSpu_name(spuObj.getString("SPU_NAME"));

                                // 5. join category3维度
                                final JSONObject category3Obj = readDim(client, "dim_base_category3", orderWide.getCategory3_id() + "");
                                orderWide.setCategory3_name(category3Obj.getString("NAME"));
                                // 6. join 品牌维度
                                final JSONObject tmObj = readDim(client, "dim_base_trademark", orderWide.getTm_id() + "");
                                orderWide.setTm_name(tmObj.getString("TM_NAME"));

                                // 把 Jedis 客户端返回给Jedis连接池   异步连接需要及时归还,否则会造成连接溢出
                                client.close();
                                // 把结果交给resultFuture
                                resultFuture.complete(Collections.singletonList(orderWide));
                            }
                        });
                    }
                },
                30,
                TimeUnit.SECONDS);

        return OrderWideWithUserInfo;

    }

    /**
     * 订单表和订单明细表join
     *
     * @param sourceStreams 消费的多个流
     * @return join后的表
     */
    private SingleOutputStreamOperator<OrderWide> factJoin(Map<String, DataStreamSource<String>> sourceStreams) {

        // 1. 解析数据, 并添加水印
        final SingleOutputStreamOperator<OrderInfo> orderInfoStream = sourceStreams
                .get("dwd_order_info")
                .map(json -> JSON.parseObject(json, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((orderInfo, ts) -> orderInfo.getCreate_ts())
                );

        final SingleOutputStreamOperator<OrderDetail> orderDetailStream = sourceStreams
                .get("dwd_order_detail")
                .map(json -> JSON.parseObject(json, OrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((orderDetail, ts) -> orderDetail.getCreate_ts())
                );

        // 2. 订单和订单明细表使用 intervalJoin 进行join
        return orderInfoStream.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
    }
}



