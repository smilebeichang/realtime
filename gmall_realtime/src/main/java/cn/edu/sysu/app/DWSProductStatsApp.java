package cn.edu.sysu.app;

import cn.edu.sysu.bean.*;
import cn.edu.sysu.util.DimAsyncFunction;
import cn.edu.sysu.util.MyKafkaUtil;
import cn.edu.sysu.util.MySinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author : song bei chang
 * @create 2021/8/6 8:48
 */
public class DWSProductStatsApp extends BaseAppV2 {

    /**
     * 业务数据、日志数据 需保证产生的日期一致
     *      dwd_page_log 包含 dwd_display_log 是的,可以灵活选择
     *
     */
    public static void main(String[] args) {
        final String[] topics = {
                "dwd_page_log",
                "dwm_order_wide",
                "dwd_favor_info",
                "dwd_cart_info",
                "dwm_payment_wide",
                "dwd_order_refund_info",
                "dwd_comment_info"
        };
        new DWSProductStatsApp().init(1, "DWSProductStatsApp", topics);
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {

        setWebUi(env, 8889);
        // 1. 解析多个流, 并合并为一个流
        final DataStream<ProductStats> productStatsDataStream = parseStreamsAndUnionOne(sourceStreams);

        // 2. 开窗聚合
        final SingleOutputStreamOperator<ProductStats> productStatsAggregateStream = aggregateByDims(productStatsDataStream);
        productStatsAggregateStream.print("结果: ");

        // 3. 补充维度信息
        SingleOutputStreamOperator<ProductStats> streamWithDim = joinDim(productStatsAggregateStream);
        streamWithDim.print("streamWithDim: ");

        // 4. 写入 sink2Clickhouse
        sink2Clickhouse(streamWithDim);

        // 重构
        sink2Kafka(streamWithDim);


    }

    /**
     * 解析各个流, 并合并一个流
     *
     * @param sourceStreams 多个输入流
     * @return 合并后的流
     */
    private DataStream<ProductStats> parseStreamsAndUnionOne(Map<String, DataStreamSource<String>> sourceStreams) {

        // 1. 转换页面及曝光流数据
        final SingleOutputStreamOperator<ProductStats> pageAndDisplayStream = sourceStreams
                .get("dwd_page_log")
                // 使用process
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String json,
                                               Context ctx,
                                               Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        // 1. 如果是商品详情
                        if (pageId.equals("good_detail")) {
                            // 1.1 获取商品的id 也就是skuId
                            Long skuId = pageJsonObj.getLong("item");
                            final ProductStats productStats = new ProductStats();
                            productStats.setTs(ts);
                            productStats.setSku_id(skuId);
                            // 商品点击数
                            productStats.setClick_ct(1L);
                            out.collect(productStats);
                        }

                        // dwd_page_log 包含 dwd_display_log，那制作dwd_display_log 的含义是什么
                        // item_type = sku_id ? 只取部分数据？
                        JSONArray displays = jsonObj.getJSONArray("display");
                        // 2. 是否包含曝光信息
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                if (display.getString("item_type").equals("sku_id")) {
                                    Long skuId = display.getLong("item");
                                    final ProductStats productStats = new ProductStats();
                                    productStats.setTs(ts);
                                    productStats.setSku_id(skuId);
                                    productStats.setDisplay_ct(1L);
                                    out.collect(productStats);
                                }
                            }
                        }

                    }
                });

        // 2. 转换下单流数据
        final SingleOutputStreamOperator<ProductStats> orderWideStream = sourceStreams
                .get("dwm_order_wide")
                .map(json -> {
                    final OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    final ProductStats productStats = new ProductStats();
                    final long ts = toTs(orderWide.getCreate_time());
                    productStats.setTs(ts);
                    productStats.setSku_id(orderWide.getSku_id());
                    productStats.setSku_name(orderWide.getSku_name());
                    productStats.getOrderIdSet().addAll(Collections.singleton(orderWide.getOrder_id()));
                    productStats.setOrder_amount(orderWide.getSplit_total_amount());
                    return productStats;
                });

        // 3. 转换收藏流数据
        final SingleOutputStreamOperator<ProductStats> favorStream = sourceStreams
                .get("dwd_favor_info")
                .map(json -> {
                    JSONObject favorInfo = JSON.parseObject(json);
                    final Long ts = toTs(favorInfo.getString("create_time"));
                    final Long skuId = favorInfo.getLong("sku_id");
                    final ProductStats productStats = new ProductStats();
                    productStats.setTs(ts);
                    productStats.setSku_id(skuId);
                    productStats.setFavor_ct(1L);
                    return productStats;
                });

        // 4. 转换购物车流数据
        final SingleOutputStreamOperator<ProductStats> cartStream = sourceStreams
                .get("dwd_cart_info")
                .map(json -> {
                    JSONObject cartInfo = JSON.parseObject(json);
                    final Long ts = toTs(cartInfo.getString("create_time"));
                    final Long skuId = cartInfo.getLong("sku_id");
                    final ProductStats productStats = new ProductStats();
                    productStats.setTs(ts);
                    productStats.setSku_id(skuId);
                    productStats.setCart_ct(1L);
                    return productStats;
                });

        // 5. 转换支付流数据
        final SingleOutputStreamOperator<ProductStats> paymentStream = sourceStreams
                .get("dwm_payment_wide")
                .map(json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    final Long ts = toTs(paymentWide.getPayment_create_time());
                    final ProductStats productStats = new ProductStats();
                    productStats.setTs(ts);
                    productStats.setSku_id(paymentWide.getSku_id());
                    productStats.setPayment_amount(paymentWide.getSplit_total_amount());
                    productStats.getPaidOrderIdSet().addAll(Collections.singleton(paymentWide.getOrder_id()));
                    return productStats;
                });

        // 6. 转换退款流数据
        final SingleOutputStreamOperator<ProductStats> refundStream = sourceStreams
                .get("dwd_order_refund_info")
                .map(json -> {
                    JSONObject refund = JSON.parseObject(json);
                    final Long ts = toTs(refund.getString("create_time"));
                    final ProductStats productStats = new ProductStats();
                    productStats.setTs(ts);
                    productStats.setSku_id(refund.getLong("sku_id"));
                    productStats.setRefund_amount(refund.getBigDecimal("refund_amount"));
                    productStats.getRefundOrderIdSet().addAll(Collections.singleton(refund.getLong("order_id")));
                    return productStats;
                });

        // 7. 转换评价流数据
        final SingleOutputStreamOperator<ProductStats> commentStream = sourceStreams
                .get("dwd_comment_info")
                .map(json -> {
                    JSONObject comment = JSON.parseObject(json);
                    final Long ts = toTs(comment.getString("create_time"));
                    final ProductStats productStats = new ProductStats();
                    productStats.setTs(ts);
                    productStats.setSku_id(comment.getLong("sku_id"));
                    productStats.setComment_ct(1L);

                    // 判断是否为好评
                    final long good_ct = comment.getString("appraise").equalsIgnoreCase(SystemConstant.APPRAISE_GOOD) ? 1L : 0L;
                    productStats.setGood_comment_ct(good_ct);
                    return productStats;
                });

        return pageAndDisplayStream.union(
                orderWideStream,
                favorStream,
                cartStream,
                paymentStream,
                refundStream,
                commentStream);

    }

    /**
     * 时间格式装换  yyyy-MM-dd HH:mm:ss
     *
     */
    public static Long toTs(String dateTime) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String toDateTimeString(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }


    /**
     * 维度聚合   ProductStats::getSku_id
     * @param productStatsDataStream
     * @return
     */
    private SingleOutputStreamOperator<ProductStats> aggregateByDims(DataStream<ProductStats> productStatsDataStream) {
        return productStatsDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((info, ts) -> info.getTs())
                )
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct((long) stats1.getOrderIdSet().size());
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct((long) stats1.getRefundOrderIdSet().size());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct((long) stats1.getPaidOrderIdSet().size());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                },
                        // 处理函数
                        // 设置 stt 和 edt  使用 new ProcessWindowFunction的ctx
                        new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong,
                                        Context context,
                                        Iterable<ProductStats> elements,
                                        Collector<ProductStats> out) throws Exception {

                        // 聚合之后, 设置下统计时间, 和更新时间戳 ts
                        // 多条数据聚合后得到一条数据, ts的值选择什么时间合适:  我们选择窗口的结束时间
                        final ProductStats productStats = elements.iterator().next();
                        productStats.setStt(toDateTimeString(context.window().getStart()));
                        productStats.setEdt(toDateTimeString(context.window().getEnd()));
                        productStats.setTs(context.window().getEnd());
                        out.collect(productStats);
                    }
                });
    }


    /**
     * 补充维度信息  AsyncDataStream.unorderedWait 和  readDim+set
     * @param productStatsAggregateStream
     * @return
     */
    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> productStatsAggregateStream) {

        final SingleOutputStreamOperator<ProductStats> result = AsyncDataStream.unorderedWait(
                productStatsAggregateStream,
                new DimAsyncFunction<ProductStats>() {
                    @Override
                    protected void addDimToInput(ProductStats input, Jedis client) {
                        // 1. 补充sku信息
                        final JSONObject skuObj = readDim(client, "dim_sku_info", input.getSku_id() + "");
                        input.setSku_name(skuObj.getString("SKU_NAME"));
                        // 写入数据时，不能有一个字段为null
                        input.setSku_price(skuObj.getBigDecimal("PRICE"));
                        input.setCategory3_id(skuObj.getLong("CATEGORY3_ID"));
                        input.setSpu_id(skuObj.getLong("SPU_ID"));
                        input.setTm_id(skuObj.getLong("TM_ID"));

                        // 2. 补充spu信息
                        final JSONObject spuObj = readDim(client, "dim_spu_info", input.getSpu_id() + "");
                        input.setSpu_name(spuObj.getString("SPU_NAME"));

                        // 3. 补充品类信息
                        final JSONObject c3Obj = readDim(client, "dim_base_category3", input.getCategory3_id() + "");
                        input.setCategory3_name(c3Obj.getString("NAME"));

                        // 4. 补充品牌信息
                        final JSONObject tmObj = readDim(client, "dim_base_trademark", input.getTm_id() + "");
                        input.setTm_name(tmObj.getString("TM_NAME"));
                    }
                },
                30,
                TimeUnit.SECONDS);
        return result;
    }


    /**
     * 写入clickHouse
     * @param resultStream
     */
    private void sink2Clickhouse(SingleOutputStreamOperator<ProductStats> resultStream) {


        resultStream.addSink(MySinkUtil.getClickHouseSink("gmall2021", "product_stats_2021", ProductStats.class));
    }


    /**
     *  重构DWSProductStatsApp, 增加最终的数据到Kafka的代码
     * @param resultStream
     */
    private void sink2Kafka(SingleOutputStreamOperator<ProductStats> resultStream) {
        resultStream
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWS_PRODUCT_STATS));
    }



}



