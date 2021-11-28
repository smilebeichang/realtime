package cn.edu.sysu.app;


import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.MyKafkaUtil;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/7/31 7:32
 */
public class DWMUserJumpDetailApp_1 extends BaseApp {

    public static void main(String[] args) throws ClassNotFoundException {

        new DWMUserJumpDetailApp_1().init(1, "DWMUserJumpApp", "dwd_page_log");

    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {

        /*sourceStream =
            env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":39999} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":50000} "
            );*/

        // 1. 数据封装, 添加水印, 按照mid分组
        final KeyedStream<JSONObject, String> jsonObjectKS = sourceStream
                .map(JSONObject::parseObject)
                // 需添加水印，默认处理时间，很快速
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        // 2. 定义模式
        final Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("go_in")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件1: 进入第一个页面 (没有上一个页面)
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        final String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件1: 进入第一个页面 (没有上一个页面)
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        final String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                // 30s后跟上条件2
                .within(Time.seconds(30));
        // 3. 把模式应用到流上
        final PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjectKS, pattern);

        // 4. 取出满足模式数据(或者超时数据)
        SingleOutputStreamOperator<JSONObject> normal = patternStream.select(
                new OutputTag<JSONObject>("timeout") {},
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern,
                                              long timeoutTimestamp) throws Exception {
                        // 超时数据, 就是跳出明细
                        return pattern.get("go_in").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("go_in").get(0); // 满足正常访问的数据, 不用返回
                    }
                }
        );

        normal
                .union(normal.getSideOutput(new OutputTag<JSONObject>("timeout") {}))
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWM_USER_JUMP_DETAIL));

    }
}


