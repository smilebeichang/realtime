package cn.edu.sysu.app;

import cn.edu.sysu.bean.ProvinceStats;
import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.bean.T3;
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
 * @create 2021/8/7 8:45
 */
public class DWSKeyWordStatsApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setWebUi(env, 2000);
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/gmall2021/flink/checkpoint2"));

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 1. 注册SourceTable: 从Kafka读数据
        // 如果是某个字段是json格式, 则把类型设置为map类型
        tenv.executeSql("CREATE TABLE page_view (" +
                "   common MAP<STRING,STRING>, " +
                "   page MAP<STRING,STRING>," +
                "   ts BIGINT, " +
                //  2021-08-04 16:21:41.000
                "   rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "   WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND " +
                ") WITH(" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'dwd_page_log'," +
                "   'properties.bootstrap.servers' = 'hadoop162:9029,hadoop163:9092,hadoop164:9092'," +
                "   'properties.group.id' = 'DWSKeyWordStatsApp'," +
                //  latest-offset  earliest-offset
                "   'scan.startup.mode' = 'earliest-offset'," +
                "   'format' = 'json'" +
                ")");

        //tenv.sqlQuery("select * from page_view").execute().print()


        // 3. 从SourceTable查询数据, 并写入到SinkTable
        // 3.1 注册自定义函数  函数名, 函数类
        tenv.createTemporaryFunction("ik_analyze", KeyWordUdtf.class);

        // 3.2 过滤出需要的数据   page['item']访问map的方法
        final Table t1 = tenv.sqlQuery("select " +
                "    page['item'] fullword," +
                "    rowtime " +
                "from page_view " +
                "where page['item'] is not null " +
                "and page['page_id'] = 'good_list'");
        tenv.createTemporaryView("t1", t1);

        // fullword 乱码是否会有影响
        //tenv.sqlQuery("select * from t1").execute().print();


        // 3.3 利用udtf进行拆分  lateral table() as T()
        final Table t2 = tenv.sqlQuery("select " +
                "    keyword, " +
                "    rowtime " +
                "from t1, " +
                "lateral table(ik_analyze(fullword)) as T(keyword)");
        tenv.createTemporaryView("t2", t2);

        tenv.sqlQuery("select * from t2").execute().print();

        // 3.4 聚合
        final Table t3 = tenv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "    keyword, " +
                "    '" + SystemConstant.KEYWORD_SEARCH + "' source, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from t2 " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword ");
        tenv.createTemporaryView("t3", t3);

        // 3.5 数据写入到ClickHouse
        //tenv.executeSql("insert into keyword_stats_2021 select * from t3")


        tenv.toRetractStream(t3, T3.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                //.print("t3：");
                .addSink(MySinkUtil.getClickHouseSink("gmall2021","keyword_stats_2021" , T3.class));


        try {
            env.execute("DWSProvinceStatsSqlApp");
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


