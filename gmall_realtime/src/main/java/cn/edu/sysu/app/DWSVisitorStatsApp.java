package cn.edu.sysu.app;

import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.bean.VisitorStats;
import cn.edu.sysu.util.MySinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/11/28 18:33
 */
public class DWSVisitorStatsApp extends BaseAppV2 {

    public static void main(String[] args) {

        new DWSVisitorStatsApp().init(1,
                "DWSVisitorStatsApp5",
                SystemConstant.DWD_PAGE_LOG,
                SystemConstant.DWM_UV,
                SystemConstant.DWM_USER_JUMP_DETAIL
        );

    }

    @Override
    protected void run(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> sourceStreams) {

        // 1. 解析3个流,并合并成一个流
        final DataStream<VisitorStats> visitorStatsDataStream = parseStreamsAndUnionOne(sourceStreams);
        //visitorStatsDataStream.print("visitorStatsDataStream")

        // 2. 开窗聚合
        SingleOutputStreamOperator<VisitorStats> resultStream = aggregateByDims(visitorStatsDataStream);
        //resultStream.print("resultStream")

        // 侧输出流验证
        resultStream.getSideOutput(new OutputTag<VisitorStats>("late"){}).print("late");

        // 3. 写入ClickHouse
        sink2ClickHouse(resultStream);

        System.out.println("完成!!");

    }

    /**
     * 解析出来3个流, 并把它们的类型map成一致的VisitorStats
     *
     * @param sourceStreams
     * @return
     */
    private DataStream<VisitorStats> parseStreamsAndUnionOne(Map<String, DataStreamSource<String>> sourceStreams) {

        // 1. 获取到3个流
        final DataStreamSource<String> pageLogStream = sourceStreams.get(SystemConstant.DWD_PAGE_LOG);
        final DataStreamSource<String> uvStream = sourceStreams.get(SystemConstant.DWM_UV);
        final DataStreamSource<String> userJumpStream = sourceStreams.get(SystemConstant.DWM_USER_JUMP_DETAIL);

        // 2. 若要把3个流合并为一个流需使用union, 其中一个会进行扩展,所以需要把它们都转成同一类型
        // uv 独立访问次数，日活
        final SingleOutputStreamOperator<VisitorStats> uvStatsStream = uvStream
                .map(json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    final JSONObject commonObj = jsonObj.getJSONObject("common");
                    return new VisitorStats("", "",
                            commonObj.getString("vc"),
                            commonObj.getString("ch"),
                            commonObj.getString("ar"),
                            commonObj.getString("is_new"),
                            1L, 0L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"),
                            jsonObj.getLong("ts"));
                });

        // pv 访问次数
        final SingleOutputStreamOperator<VisitorStats> pageStatsStream = pageLogStream
                .map(json -> {
                    final JSONObject jsonObj = JSON.parseObject(json, JSONObject.class);
                    final JSONObject commonObj = jsonObj.getJSONObject("common");
                    return new VisitorStats("", "",
                            commonObj.getString("vc"),
                            commonObj.getString("ch"),
                            commonObj.getString("ar"),
                            commonObj.getString("is_new"),
                            0L, 1L, 0L, 0L, 0L,
                            jsonObj.getLong("ts"));
                });


        // sv 跳入次数  flatMap
        final SingleOutputStreamOperator<VisitorStats> svPageStream = pageLogStream
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String json, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        final JSONObject commonObj = jsonObj.getJSONObject("common");
                        final String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        // lastPageId = null 即为进入首页
                        if (lastPageId == null || lastPageId.length() == 0) {

                            final VisitorStats visitorStats = new VisitorStats("", "",
                                    commonObj.getString("vc"),
                                    commonObj.getString("ch"),
                                    commonObj.getString("ar"),
                                    commonObj.getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L,
                                    jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });

        // uj 跳出次数
        final SingleOutputStreamOperator<VisitorStats> userJumpStatsStream = userJumpStream.map(json -> {
            final JSONObject jsonObj = JSON.parseObject(json, JSONObject.class);
            final JSONObject commonObj = jsonObj.getJSONObject("common");
            return new VisitorStats("", "",
                    commonObj.getString("vc"),
                    commonObj.getString("ch"),
                    commonObj.getString("ar"),
                    commonObj.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObj.getLong("ts"));
        });

        return uvStatsStream.union(pageStatsStream, svPageStream, userJumpStatsStream);

    }



    /**
     * 进行聚合
     *
     *      现象：UV为0
     *      排查：使用侧输出流，打印侧输出流是否有数据
     *      解决：1.乱序时间需大于 uv的时间
     *           2.每一个参与union的流均加一个窗口，默认选最小的窗口
     *
     */
    private SingleOutputStreamOperator<VisitorStats> aggregateByDims(DataStream<VisitorStats> visitorStatsDataStream) {

        final SingleOutputStreamOperator<VisitorStats> result = visitorStatsDataStream

                // 1. 添加水印和时间戳
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 乱序时间需大于 uv的时间
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                )

                // 2. 选取4个维度做为key:版本, 渠道, 地区, 新老用户标识
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                // 使用滚动窗口,避免重叠
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 加入侧输出流验证是否有过期数据
                .sideOutputLateData(new OutputTag<VisitorStats>("late"){})

                // 3. 聚合
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats stats1,
                                                       VisitorStats stats2) throws Exception {
                                stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                                stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                                stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                                stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                                stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                                return stats1;
                            }
                        },
                        // 使用 ProcessWindowFunction 添加时间戳字段
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {

                            private SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                            @Override
                            public void process(String key,
                                                Context context,
                                                Iterable<VisitorStats> elements,
                                                Collector<VisitorStats> out) throws Exception {
                                final VisitorStats stats = elements.iterator().next();
                                // 补充统计时间
                                stats.setStt(f.format(new Date(context.window().getStart())));
                                stats.setEdt(f.format(new Date(context.window().getEnd())));
                                out.collect(stats);
                            }
                        });
        result.print("结果输出:");
        return result;
    }


    /**
     * 写入ClickHouse
     * @param statsStream
     *      初步定位:1.没有进入该方法   2.bcc3连不上
     */
    private void sink2ClickHouse(SingleOutputStreamOperator<VisitorStats> statsStream) {

        statsStream.addSink(MySinkUtil.getClickHouseSink("gmall_realtime", "visitor_stats_2021", VisitorStats.class));

    }


}



