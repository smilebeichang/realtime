package cn.edu.sysu.app;

import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author : song bei chang
 * @create 2021/11/27 10:09
 *
 *      对日志数据进行分流, 写入到dwd层(kafka)
 *
 *      1. 对新老用户进行确认
 *
 *      2. 对ods_log流进行分流(使用侧输出流)
 *          不同的数据放入不同的流
 *         启动日志
 *         曝光日志
 *         页面日志
 *
 *      3. 把数据写入到Kafka中
 */
public class DWDLogApp extends BaseApp {

    final String START_STREAM = "start";
    final String PAGE_STREAM = "page";
    final String DISPLAY_STREAM = "displays";


    public static void main(String[] args) throws ClassNotFoundException, IOException {

        new DWDLogApp().init(1, "DWDLogApp", SystemConstant.TOPIC_ODS_LOG);

    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       DataStreamSource<String> sourceStream) {

        sourceStream.print();
        // 1. 识别新老客户: 在日志数据中修改一个字段用来表示是否新老客户
        SingleOutputStreamOperator<JSONObject> validateFlagDS = distinguishNewOrOld(sourceStream);

        // 2. 流的拆分: 利用侧输出流
        Tuple3<DataStream<String>, DataStream<String>, DataStream<String>> streams = splitStream(validateFlagDS);

        String hadoop_user_name = System.getProperty("HADOOP_USER_NAME");
        System.out.println("HADOOP_USER_NAME: "+hadoop_user_name);

        // 3. 分别写入到不同的Topic中 三元元组Tuple3
        sendToKafka(streams);

    }


    /**
     * 三元元组  使用.f0 .f1 .f2 获取
     *
     */
    private void sendToKafka(Tuple3<DataStream<String>, DataStream<String>, DataStream<String>> streams) {

        streams.f0.addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWD_PAGE_LOG));
        streams.f1.addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWD_START_LOG));
        streams.f2.addSink(MyKafkaUtil.getKafkaSink(SystemConstant.DWD_DISPLAY_LOG));

    }

    /**
     * 2. 流的拆分: 利用侧输出流
     *
     */
    private Tuple3<DataStream<String>, DataStream<String>, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {

        OutputTag<String> startTag = new OutputTag<String>("startStream") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayStream") {};

        SingleOutputStreamOperator<String> pageStream = stream.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value,
                                       Context ctx,
                                       Collector<String> out) throws Exception {

                // 1. 获取启动数据
                JSONObject start = value.getJSONObject(START_STREAM);
                if (start != null) {
                    // 2. 如果是启动日志, 则把数据放入启动侧输出流
                    ctx.output(startTag, value.toJSONString());

                } else {
                    // 3. 如果非启动日志, 则为页面日志或者曝光数据或者既是页面又是曝光
                    // 3.1 是否为页面日志
                    final JSONObject page = value.getJSONObject(PAGE_STREAM);
                    if (page != null) {
                        // 页面日志为主流
                        out.collect(value.toJSONString());
                    }

                    // 3.2 获取曝光信息, 每个曝光信息为流中的一条数据
                    JSONArray displays = value.getJSONArray(DISPLAY_STREAM);
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            // 根据下标可以获取到个体   因为display 是JSONArray
                            JSONObject display = displays.getJSONObject(i);
                            // 给display补充
                            // 补充pageId
                            String pageId = value.getJSONObject(PAGE_STREAM).getString("page_id");
                            display.put("page_id", pageId);

                            // 补充时间戳
                            display.put("ts", value.getLong("ts"));

                            // 补充common所有字段
                            display.putAll(value.getJSONObject("common"));

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);

        // 主流和侧输出流是 平行的关系,但是是由主流创建出来的
        //      1.接收窗口关闭之后的迟到数据 .sideOutputLateData
        //      2.使用侧输出流把一个流拆成多个流
        // 或者可以返回Map
        return Tuple3.of(pageStream, startStream, displayStream);
    }


    /**
     * 1. 识别新老客户
     *
     */
    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> sourceStream) {

        SingleOutputStreamOperator<JSONObject> result = sourceStream
                // 数据解析成JSONObject对象
                .map(JSON::parseObject)
                // 添加水印和事件时间
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts"))
                )
                // 按照mid进行分组
                .keyBy(jsonData -> jsonData.getJSONObject("common").getString("mid"))
                // 添加滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    // 存储是是首次访问的时间戳
                    private ValueState<Long> firstVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("firstVisitState", Long.class));
                    }

                    @Override
                    public void process(String mid,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        if (firstVisitState.value() == null) {
                            // 1.  如果是这个 mid 的首个窗口, 则把时间戳最小的设置首次访问, 其他均为非首次访问
                            ArrayList<JSONObject> list = new ArrayList<>();
                            for (JSONObject element : elements) {
                                list.add(element);
                            }
                            list.sort(Comparator.comparing(o -> o.getLong("ts")));

                            for (int i = 0; i < list.size(); i++) {
                                if (i == 0) {
                                    list.get(i).getJSONObject("common").put("is_new", "1");
                                    firstVisitState.update(list.get(i).getLong("ts"));
                                } else {
                                    list.get(i).getJSONObject("common").put("is_new", "0");
                                }
                                out.collect(list.get(i));
                            }
                        } else {
                            // 2. 如果不是这个mid的首个窗口, 则所有均为非首次访问
                            elements.forEach(data -> {
                                data.getJSONObject("common").put("is_new", "0");
                                out.collect(data);
                            });
                        }
                    }
                });
        return result;
    }



}



