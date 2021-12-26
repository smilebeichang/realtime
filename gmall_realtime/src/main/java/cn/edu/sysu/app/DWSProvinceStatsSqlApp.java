package cn.edu.sysu.app;

import cn.edu.sysu.bean.OrderWide;
import cn.edu.sysu.bean.ProvinceStats;
import cn.edu.sysu.util.MySinkUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/29 21:56
 *
 * 没有继承BaseApp 一切均自己搞定
 */
public class DWSProvinceStatsSqlApp {


    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://ecs2:9820/flink/realtime/checkpoint"));

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1. 注册SourceTable: 从Kafka读数据
        tenv.executeSql("CREATE TABLE order_wide (" +
                "   province_id BIGINT, " +
                "   province_name STRING," +
                "   province_area_code STRING," +
                "   province_iso_code STRING," +
                "   province_3166_2_code STRING," +
                "   order_id STRING, " +
                "   split_total_amount DOUBLE," +
                "   create_time STRING, " +
                "   rowtime AS TO_TIMESTAMP(create_time)," +
                "   WATERMARK FOR  rowtime  AS rowtime - interval '5' second )" +
                "WITH (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'dwm_order_wide'," +
                "   'properties.bootstrap.servers' = 'ecs2:9092,ecs3:9092,ecs4:9092'," +
                "   'properties.group.id' = 'DWSProvinceStatsSqlApp3'," +
                // latest-offset  earliest-offset
                "   'scan.startup.mode' = 'earliest-offset'," +
                "   'format' = 'json'" +
                ")");

        // 3. 从SourceTable查询数据, 并写入到SinkTable
        //tenv.sqlQuery("select * from order_wide").execute().print();
        Table tableResult = tenv.sqlQuery(" " +
                "select " +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                "   province_id," +
                "   province_name," +
                "   province_area_code area_code," +
                "   province_iso_code iso_code," +
                "   province_3166_2_code iso_3166_2 ," +
                "   sum(split_total_amount) order_amount," +
                "   COUNT(DISTINCT order_id) order_count, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from  order_wide " +
                "group by  " +
                "   TUMBLE(rowtime, INTERVAL '10' SECOND ), " +
                "   province_id," +
                "   province_name," +
                "   province_area_code," +
                "   province_iso_code," +
                "   province_3166_2_code ");

        tenv.toRetractStream(tableResult, ProvinceStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                //.print("pv：");
                .addSink(MySinkUtil.getClickHouseSink("gmall_realtime","province_stats_2021" , ProvinceStats.class));



        try {
            env.execute("DWSProvinceStatsSqlApp");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}


