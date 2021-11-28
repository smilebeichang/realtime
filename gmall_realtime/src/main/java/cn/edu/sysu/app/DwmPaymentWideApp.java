package cn.edu.sysu.app;

import cn.edu.sysu.bean.OrderWide;
import cn.edu.sysu.bean.PaymentInfo;
import cn.edu.sysu.bean.PaymentWide;
import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.CommonUtil;
import cn.edu.sysu.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/8/3 9:15
 *
 * 双流join所以继承 BaseAppV2
 */
public class DwmPaymentWideApp extends BaseAppV2 {

    public static void main(String[] args) {
        new DwmPaymentWideApp().init(1, "DWMPaymentWideApp", SystemConstant.TOPIC_DWD_PAYMENT_INFO, SystemConstant.TOPIC_DWM_ORDER_WIDE);
    }

    
    @Override
    protected void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> streams) {

        // 1.获取流 支付
        KeyedStream<PaymentInfo, Long> paymentInfoStream = streams
            .get(SystemConstant.TOPIC_DWD_PAYMENT_INFO)
            .map(s -> JSON.parseObject(s, PaymentInfo.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((pi, ts) -> CommonUtil.toTs(pi.getCreate_time()))
            )
            .keyBy(PaymentInfo::getOrder_id);
        paymentInfoStream.print("paymentInfoStream");

        // 2.获取流  订单宽表
        KeyedStream<OrderWide, Long> orderWideStream = streams
            .get(SystemConstant.TOPIC_DWM_ORDER_WIDE)
            .map(s -> JSON.parseObject(s, OrderWide.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ow, ts) -> CommonUtil.toTs(ow.getCreate_time()))
            )
            .keyBy(OrderWide::getOrder_id);
        orderWideStream.print("orderWideStream");


        // 3.双流 intervalJoin
        SingleOutputStreamOperator<PaymentWide> paymentWideStream = paymentInfoStream
                .intervalJoin(orderWideStream)
                .between(Time.minutes(-45), Time.seconds(10))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left,
                                               OrderWide right,
                                               Context ctx,
                                               Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });


        // 4.数据写入到 Kafka
        paymentWideStream
                // 转为 JSON 字符串,方便后续调用
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWM_PAYMENT_WIDE));
        
    }
}
