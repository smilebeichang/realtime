package cn.edu.sysu.app;

import cn.edu.sysu.util.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/27 10:09
 *
 *              47.119.157.0 ecs2
 *              47.119.169.212 ecs3
 *              8.135.102.7 ecs4
 *
 *              106.12.138.166 bcc2
 *              182.61.59.65 bcc3
 *              106.12.138.245 bcc4
 */
public abstract class BaseApp {

    /**
     * 子类在此抽象方法中完成自己的业务逻辑
     *
     * @param env          执行环境
     * @param sourceStream 从Kafka直接获取得到的流
     */
    protected abstract void run(StreamExecutionEnvironment env,
                                DataStreamSource<String> sourceStream) throws ClassNotFoundException;

    /**
     * 做初始化相关工作
     *
     * @param defaultParallelism 默认并行度
     * @param groupId            消费者组
     * @param topic              消费的topic
     */
    public void init(int defaultParallelism, String groupId, String topic) throws ClassNotFoundException {

        // 设置不生效,会被本地变量覆盖
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
        env.setStateBackend(new FsStateBackend("hdfs://ecs2:9820/flink/realtime/checkpoint"));

        DataStreamSource<String> sourceStream = env.addSource(MyKafkaUtil.getKafkaSource(groupId, topic));

        run(env, sourceStream);

        // executor 构造器,可以给取jobName
        try {
            env.execute(groupId);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}



