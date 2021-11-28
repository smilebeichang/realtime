package cn.edu.sysu.app;


import cn.edu.sysu.util.MyKafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author : song bei chang
 * @create 2021/7/31 7:38
 */
public abstract class BaseAppV2 {
    /**
     * 子类在此抽象方法中完成自己的业务逻辑
     *
     * @param env           执行环境
     * @param sourceStreams 从Kafka直接获取得到的多个流
     */
    protected abstract void run(StreamExecutionEnvironment env,
                                Map<String, DataStreamSource<String>> sourceStreams);

    /**
     * 做初始化相关工作
     *
     * @param defaultParallelism 默认并行度
     * @param groupId            消费者组
     * @param topics             消费的的多个流
     */
    public void init(int defaultParallelism, String groupId, String... topics) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(defaultParallelism);
        // 设置CK相关的参数
        // 1. 设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 2. Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 3.开启在 job 中止后仍然保留的 externalized checkpoints
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4. 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/gmall2021/flink/checkpoint1"));

        final Map<String, DataStreamSource<String>> sourceStreams = new HashMap<>();
        for (String topic : topics) {
            final DataStreamSource<String> sourceStream = env.addSource(MyKafkaUtil.getKafkaSource(groupId, topic));
            sourceStreams.put(topic, sourceStream);
        }
        run(env, sourceStreams);
        try {
            env.execute(groupId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 修改webui的端口. 在idea调试的时候, 方便观察执行情况
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



