package cn.edu.sysu.app;

import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
/**
 * @Author : song bei chang
 * @create 2021/7/31 7:29
 *      UV  日活
 *      firstVisitTsState  状态值为null 或者 今天时间与状态的时间不一致
 */
public class DWMUVApp extends BaseApp {

    public static void main(String[] args) throws ClassNotFoundException {
        new DWMUVApp().init(2, "DWMUVApp", "dwd_page_log");
    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {
        sourceStream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private ValueStateDescriptor<Long> firstVisitTsDescriptor;
                    private ValueState<Long> firstVisitTsState;
                    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitTsDescriptor = new ValueStateDescriptor<>("firstVisitTs", Long.class);
                        firstVisitTsState = getRuntimeContext().getState(firstVisitTsDescriptor);

                    }

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {

                        // 思路: 什么情况下才算是某个用户的今天第一个窗口
                        // 答: 状态值为null 或者 今天时间与状态的时间不一致

                        // 因为是event-time 所以使用使用水印来表示当前时间
                        final String now = sdf.format(new Date(context.currentWatermark()));
                        if (firstVisitTsState.value() == null || !now.equals(sdf.format(firstVisitTsState.value()))) {
                            // 找到时间戳最小的那个
                            final ArrayList<JSONObject> list = Lists.newArrayList(elements);
                            final JSONObject min = Collections.min(list, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    final Long ts1 = o1.getLong("ts");
                                    final Long ts2 = o2.getLong("ts");
                                    return ts1.compareTo(ts2);
                                }
                            });
                            // 把具有最小时间戳的记录发送到下游
                            out.collect(min);
                            // 更新状态
                            firstVisitTsState.update(min.getLong("ts"));
                        }
                    }
                })
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWM_UV));

    }
}



