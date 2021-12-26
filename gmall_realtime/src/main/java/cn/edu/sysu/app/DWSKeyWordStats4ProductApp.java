package cn.edu.sysu.app;

import cn.edu.sysu.bean.T3;
import cn.edu.sysu.udf.KeyWordProductUdtf;
import cn.edu.sysu.udf.KeyWordUdtf;
import cn.edu.sysu.util.MySinkUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.Field;

/**
 * @Author : song bei chang
 * @create 2021/11/29 22:42
 */
public class DWSKeyWordStats4ProductApp {


    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setWebUi(env, 2100);
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://ecs2:9820/flink/realtime/checkpoint"));

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 注册SourceTable: 从Kafka读数据  由于dws_product_stats聚合的时候已经有了窗口, 此处不用再开窗
        tenv.executeSql("CREATE TABLE product_stats (" +
                "   spu_name STRING, " +
                "   click_ct BIGINT," +
                "   cart_ct BIGINT," +
                "   order_ct BIGINT," +
                "   stt STRING," +
                "   edt STRING" +
                ") WITH(" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'dws_product_stats'," +
                "   'properties.bootstrap.servers' = 'ecs2:9092,ecs3:9092,ecs4:9092'," +
                "   'properties.group.id' = 'DWSKeyWordStats4ProductApp'," +
                //  latest-offset  earliest-offset
                "   'scan.startup.mode' = 'earliest-offset'," +
                "   'format' = 'json'" +
                ")");


        // 3. 从SourceTable查询数据, 并写入到SinkTable
        // 3.1 注册自定义函数
        tenv.createTemporaryFunction("ik_analyze", KeyWordUdtf.class);
        tenv.createTemporaryFunction("KeyWordProduct", KeyWordProductUdtf.class);

        // 3.2 聚合计算
        final Table t1 = tenv.sqlQuery("select " +
                "    stt," +
                "    edt, " +
                "    keyword," +
                "    source, " +
                "    ct," +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from product_stats, " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(KeyWordProduct(click_ct,cart_ct,order_ct)) as T2(source, ct)");
        tenv.createTemporaryView("t1", t1);
        //tenv.sqlQuery("select * from  t1").execute().print();

        // 3.3 数据写入到ClickHouse中  keyword_stats_2021  为什么写入keyword_stats_2021  237 好像写不进去
        tenv.toRetractStream(t1, T3.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                //.print("t1：");
                .addSink(MySinkUtil.getClickHouseSink("gmall_realtime","keyword_stats_2021" , T3.class));


        try {
            env.execute("DWSKeyWordStats4ProductApp");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    public static void setWebUi(StreamExecutionEnvironment env, int port) {
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



