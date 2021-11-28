package cn.edu.sysu.app;


import cn.edu.sysu.bean.OrderDetail;
import cn.edu.sysu.bean.OrderInfo;
import cn.edu.sysu.bean.OrderWide;
import cn.edu.sysu.util.MyJDBCUtil;
import cn.edu.sysu.util.MyRedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/7/31 7:39
 */
public class DWMOrderWideApp extends BaseAppV2 {

    public static void main(String[] args) {
        new DWMOrderWideApp().init(2,
                "DWMOrderWideApp",
                "dwd_order_info",
                "dwd_order_detail");
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {

        setWebUi(env, 8888);
        // 1. 订单表和订单明细表join
        final SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim = factJoin(sourceStreams);

        // orderWideStreamWithoutDim.print("join")

        // 2. join维度信息
        final SingleOutputStreamOperator<OrderWide> orderWideStreamWithDim = readDim(orderWideStreamWithoutDim);

        // orderWideStreamWithDim.print("dim")

    }


    /**
     * join 维度数据
     *
     * @param orderWideStreamWithoutDim 没有维度信息的流
     * @return SingleOutputStreamOperator<OrderWide> join维度表之后的流
     */
    private SingleOutputStreamOperator<OrderWide> readDim(SingleOutputStreamOperator<OrderWide> orderWideStreamWithoutDim) {

        String phoenixUrl = "jdbc:phoenix:ecs2,ecs3,ecs4:2181";

        // 1. join 维度信息
        final SingleOutputStreamOperator<OrderWide> result = orderWideStreamWithoutDim
                .map(new RichMapFunction<OrderWide, OrderWide>() {

                    private Connection conn;
                    private Jedis client;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = MyJDBCUtil.getConnection(phoenixUrl);
                        client = MyRedisUtil.getClient();
                        // 可以选择库，避免key过多，对其他业务产生影响
                        client.select(1);
                    }

                    // 方法嵌套同名方法  方法的重载  自动生成的吗
                    private JSONObject readDim(String table, String id) {
                        String sql = "select * from " + table + " where id=?";
                        String key = "dim_" + table + "_" + id;

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

                        } else {
                            System.out.println("走数据库..." + key  );
                            //1.2  缓存中没有数据, 需要从HBase读取数据
                            final List<JSONObject> dim = MyJDBCUtil
                                    .queryList(conn, sql, new Object[]{id}, JSONObject.class, false);
                            obj = dim.get(0);
                            // 1.3 数据写入缓存, 设置缓存失效时间7天 key,ddl,value
                            client.setex(key, 3600 * 24 * 7, JSON.toJSONString(obj));
                        }
                        return obj;
                    }

                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        // 1. join user_info
                        final JSONObject userInfoObj = readDim("dim_user_info", orderWide.getUser_id().toString());
                        orderWide.setUser_gender(userInfoObj.getString("GENDER"));
                        orderWide.calcUserAgeByBirthday(userInfoObj.getString("BIRTHDAY"));

                        // 2. join 省份维度
                        final JSONObject provinceObj = readDim("dim_base_province", orderWide.getProvince_id().toString());
                        orderWide.setProvince_3166_2_code(provinceObj.getString("ISO_3166_2"));
                        orderWide.setProvince_area_code(provinceObj.getString("AREA_CODE"));
                        orderWide.setProvince_name(provinceObj.getString("NAME"));
                        orderWide.setProvince_iso_code(provinceObj.getString("ISO_CODE"));

                        // 3. join sku维度
                        final JSONObject skuObj = readDim("dim_sku_info", orderWide.getSku_id() + "");
                        orderWide.setSku_name(skuObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(skuObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(skuObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(skuObj.getLong("TM_ID"));

                        // 4. join spu维度
                        final JSONObject spuObj = readDim("dim_spu_info", orderWide.getSpu_id() + "");
                        orderWide.setSpu_name(spuObj.getString("SPU_NAME"));

                        // 5. join category3维度
                        final JSONObject category3Obj = readDim("dim_base_category3", orderWide.getCategory3_id() + "");
                        orderWide.setCategory3_name(category3Obj.getString("NAME"));

                        // 6. join 品牌维度
                        final JSONObject tmObj = readDim("dim_base_trademark", orderWide.getTm_id() + "");
                        orderWide.setTm_name(tmObj.getString("TM_NAME"));

                        // 不应该在这归还|关闭redis,将会导致redisPool出现配置不一致的问题
                        // client.close()

                        return orderWide;
                    }

                    @Override
                    public void close() throws Exception {
                        if (conn != null) {
                            conn.close();
                        }
                        if (client != null) {
                            client.close();
                        }
                    }
                });
        return result;

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



