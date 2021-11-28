package cn.edu.sysu.app;


import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/7/31 7:32
 *  用户跳出数
 */
public class DWMUserJumpDetailApp extends BaseApp {

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        new DWMUserJumpDetailApp().init(1, "DWMUserJumpApp", "dwd_page_log");

    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {

        // 模拟数据,并覆盖流
            /*sourceStream =
            env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":39999} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":50000} "
            );*/

        setWebUi(env, 20001);

        sourceStream.print();

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
                // 入口
                .<JSONObject>begin("go_in")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件1: 进入第一个页面 (没有上一个页面)
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        final String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                // 接下来30s的行为
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    // 条件2: 一个或多个访问记录
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        final String pageId = value.getJSONObject("page").getString("page_id");
                        return pageId != null && pageId.length() > 0;
                    }
                })
                // 30s后跟上条件2
                .within(Time.seconds(30));

        // 3. 把模式应用到流上
        final PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjectKS, pattern);

        // 4. 获取匹配到的结果: 提前超时数据
        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};
        final SingleOutputStreamOperator<Object> resultStream = patternStream.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern,
                                        long timeoutTimestamp,
                                        Collector<String> out) throws Exception {
                        // 超时的数据放入侧输出流
                        final List<JSONObject> objList = pattern.get("go_in");
                        for (JSONObject obj : objList) {
                            out.collect(obj.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern,
                                           Collector<Object> out) throws Exception {
                        // 正常数据不需要, 所以此处代码省略
                    }
                });

        // 5. 把侧输出流的结果写入到dwm_user_jump_detail
        final DataStream<String> jumpStream = resultStream.getSideOutput(timeoutTag);
        jumpStream.print("jump:");
        jumpStream.addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWM_USER_JUMP_DETAIL));

    }


    /** 修改webui的端口. 在idea调试的时候, 方便观察执行情况 */
    public void setWebUi(StreamExecutionEnvironment env, int port) {
        try {
            final Field field = StreamExecutionEnvironment.class.getDeclaredField("configuration");
            field.setAccessible(true);
            final Configuration config = (Configuration) field.get(env);
            config.setInteger("rest.port", port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


