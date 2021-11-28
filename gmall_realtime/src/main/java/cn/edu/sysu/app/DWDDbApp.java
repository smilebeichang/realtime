package cn.edu.sysu.app;

import cn.edu.sysu.bean.SystemConstant;
import cn.edu.sysu.bean.TableProcess;
import cn.edu.sysu.util.MyKafkaUtil;
import cn.edu.sysu.util.MyRedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/11/27 22:46
 *
 *  对业务数据进行分流, 写入到dwd层(kafka 和 hbase)
 *  java -jar 只有user_info 维度发生了变化
 *
 */
public class DWDDbApp extends BaseApp {


    final static OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbaseTag") {};


    public static void main(String[] args) throws ClassNotFoundException {

        new DWDDbApp().init(1, "DWDDbApp", SystemConstant.TOPIC_ODS_DB);

    }


    @Override
    public void run(StreamExecutionEnvironment env,
                    DataStreamSource<String> sourceStream) throws ClassNotFoundException {




        // 1. 对流进行etl过滤
        final SingleOutputStreamOperator<JSONObject> etlStream = etl(sourceStream);


        // 2. 读取配置表数据, 使用cdc把数据做成流
        final SingleOutputStreamOperator<TableProcess> processTableStream = readProcessTable(env);

        processTableStream.print();

        // 3. 根据配置表数据进行动态分流  事实表 维度表
        final SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> sink2KafkaStream = dynamicSplit(etlStream, processTableStream);

        // 获取侧输出流
        final DataStream<Tuple2<JSONObject, TableProcess>> sink2HbaseStream = sink2KafkaStream.getSideOutput(hbaseTag);

        // 4. sink数据   java -jar 到kafka   maxwell-bootstrap 到hbase
        sink2Kafka(sink2KafkaStream);

        sink2Hbase(sink2HbaseStream);

    }


    /**
     * 事实表数据写入到Kafka
     *
     * @param sink2KafkaStream 事实表数据流
     *
     *         // 声明为全局变量快捷键 ctrl + alt + f
     *         String s =  "233";
     */
    private void sink2Kafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> sink2KafkaStream) {
        // 升级版:不传topic
        // 根据内容动态的写入不同的kafka Topic  (本质是提取SinkTable)
        sink2KafkaStream.addSink(MyKafkaUtil.getKafkaSink());
    }


    /**
     * 维度表数据写入到Hbase
     *
     * @param sink2HbaseStream 维度表数据流
     */
    private void sink2Hbase(DataStream<Tuple2<JSONObject, TableProcess>> sink2HbaseStream) throws ClassNotFoundException {

        // 最好配置一个驱动全类名 1.13版本已经修复
        // 先加载驱动, 很多情况不是必须. 大部分常用的数据库会根据url自动选择合适的driver.   Phoenix 驱动有些时候需要手动加载一下
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String phoenixUrl = "jdbc:phoenix:ecs2,ecs3,ecs4:2181";
        sink2HbaseStream
                // 按照要sink的表进行分组
                .keyBy(f -> f.f1.getSinkTable())
                .addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcess>>() {

                    private Connection conn;
                    private ValueState<Boolean> tableCreated;
                    private Jedis client;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open ...");
                        // 建立到Phoenix的连接
                        conn = DriverManager.getConnection(phoenixUrl);
                        // 定义一个状态, 用来表示该表是否已经被创建
                        tableCreated = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Boolean>("tableCreated", Boolean.class));

                        // redis的意义是什么?
                        client = MyRedisUtil.getClient();
                        client.select(1);
                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
                        System.out.println("invoke ...");
                        //1. 检测表是否存在
                        checkTable(value);
                        //2. 向Phoenix中插入数据
                        // upset into user(id,name) values('100', 'lisi')
                        write2Hbase(value);

                        // 事实表join维度表的时候添加的代码
                        delCache(value);
                    }

                    private void delCache(Tuple2<JSONObject, TableProcess> value) {
                        final JSONObject obj = value.f0.getJSONObject("data");
                        final TableProcess tp = value.f1;
                        // 只处理更新的信息
                        if ("update".equalsIgnoreCase(tp.getOperateType())) {
                            client.del(tp.getSinkTable() + "_" + obj.getString("id"));
                        }
                        // 更新或者删除均可 更新24h后删除，删除保证数据一定会使用，读起来是快，但冷数据没必要存，redis受不了。Update
                    }

                    /**
                     * 数据写入到Hbase中
                     * @param value 要写入的相关数据
                     * @throws SQLException
                     */
                    private void write2Hbase(Tuple2<JSONObject, TableProcess> value) throws SQLException {
                        final JSONObject dataJson = value.f0.getJSONObject("data");
                        final TableProcess tp = value.f1;

                        // 2.1 拼接sql语句
                        final StringBuilder upsertSql = new StringBuilder();
                        upsertSql
                                .append("upsert into ")
                                .append(tp.getSinkTable())
                                .append("(")
                                .append(value.f1.getSinkColumns())
                                .append(") values (");

                        //  拼接需要单引号,Json.get(字段名)
                        //  此处可以使用正则 来进行简单的替换
                        for (String column : tp.getSinkColumns().split(",")) {
                            upsertSql.append("'").append(dataJson.getString(column)).append("',");
                        }
                        //  将最后一个，删除
                        upsertSql.deleteCharAt(upsertSql.length() - 1);
                        upsertSql.append(")");

                        //  老师的一个小时课程，可以抵挡我们90%的需求

                        // 2.2 执行sql语句
                        final PreparedStatement ps;
                        try {
                            System.out.println(upsertSql);
                            ps = conn.prepareStatement(upsertSql.toString());
                            ps.execute();
                            conn.commit();
                            ps.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

                    /**
                     * 检测表是否存在, 如果不存在则先在Phoenix中创建表
                     * @param value
                     * @throws Exception
                     */
                    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        // 通过状态，避免频繁建立链接，提高性能
                        // 为什么只有一张表，user_info   java -jar 会各种表才对啊
                        if (tableCreated.value() == null) {
                            System.out.println("checkTable -> if ");
                            tableCreated.update(true);
                            // 表示第一次插入数据, 需要首先在Phoenix中创建表
                            // 生成建表语句:  create table if not exists user(id varchar, name varchar , constraint pk primary key(id, name) ) SALT_BUCKETS = 3
                            final StringBuilder createSql = new StringBuilder();
                            createSql
                                    .append("create table if not exists ")
                                    .append(value.f1.getSinkTable())
                                    .append("(");
                            //  最后一个,不需要去除
                            for (String column : value.f1.getSinkColumns().split(",")) {
                                createSql.append(column).append(" varchar,");
                            }

                            createSql
                                    .append("constraint pk primary key(")
                                    .append(value.f1.getSinkPk() == null ? "id" : value.f1.getSinkPk())
                                    .append(")");
                            createSql.append(")");

                            // phoenix 的切分region   盐表
                            createSql.append(value.f1.getSinkExtend() == null ? "" : value.f1.getSinkExtend());

                            PreparedStatement ps = conn.prepareStatement(createSql.toString());
                            ps.execute();
                            // 主动提交
                            conn.commit();
                            ps.close();

                        }
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("close...");
                        if (conn != null) {
                            conn.close();
                        }
                    }
                });

    }

    /**
     * 对db数据进行etl
     *      根据实际业务, 对数据做一些过滤（库 表 类型 数据）
     *
     */
    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> sourceStream) {

        return sourceStream
                .map(JSON::parseObject)
                .filter(value -> value.getString("table") != null
                        && value.getJSONObject("data") != null
                        && value.getString("data").length() > 3
                && (value.getString("type").contains("insert") || "update".equals(value.getString("type"))))
                ;

    }



    /**
     * 读取的配置表的数据  使用flink-sql-cdc来完成
     *      stream/table 两种方式均可  https://github.com/ververica/flink-cdc-connectors
     *      env 全局只要这个方法在使用，故上层传递过来即可
     */
    private SingleOutputStreamOperator<TableProcess> readProcessTable(StreamExecutionEnvironment env) {

        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv
                .executeSql("CREATE TABLE `table_process`( " +
                        "   `source_table`  string, " +
                        "   `operate_type`  string, " +
                        "   `sink_type`  string, " +
                        "   `sink_table`  string, " +
                        "   `sink_columns` string, " +
                        "   `sink_pk`  string, " +
                        "   `sink_extend`  string, " +
                        "   PRIMARY KEY (`source_table`,`operate_type`)  NOT ENFORCED" +
                        ")with(" +
                        "   'connector' = 'mysql-cdc', " +
                        "   'hostname' = 'ecs2', " +
                        "   'port' = '3306', " +
                        "   'username' = 'root', " +
                        "   'password' = 'sbc006688', " +
                        "   'database-name' = 'gmall_realtime', " +
                        "   'table-name' = 'table_process'," +
                        "   'debezium.snapshot.mode' = 'initial' " +  // 读取mysql的全量,增量
                        ")");
        // cdc 一个参数决定了是否查询以往数据  debezium.snapshot.mode
        // initial: 启动的时候会读取表中所有的数据, 放在内存中, 全部数据读取完成之后, 然后继续使用binlog来监控mysql的变化.  默认值
        // never:   只用binlog来监控mysql的变化

        // 使用sql别名方式 解决驼峰命名法和下划线命名法存在差异  也可以改变bean的属性名,为下面的转换做准备
        final Table table = tenv.sqlQuery("select " +
                "  source_table sourceTable, " +
                "  sink_type sinkType, " +
                "  operate_type operateType, " +
                "  sink_table sinkTable, " +
                "  sink_columns sinkColumns, " +
                "  sink_pk sinkPk, " +
                "  sink_extend sinkExtend " +
                "from table_process ");

        // 将+D(撤回的)的数据过滤,并只保留+I(insert)部分
        return tenv
                .toRetractStream(table, TableProcess.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);
    }


    /**
     * 对数据流进行动态切分
     *
     * @param dbStream           业务数据流
     * @param processTableStream 动态配置流
     * @return 到kafka的主流
     *
     *
     *      动态分流
     *          最终目标: 应该得到一个新的流, 新的流存储的数据类型应该是一个二维元组
     *          Tuple2<JSONObject, TableProcess>
     *
     *      中间步骤:碰到一条数据流中的数据, 找一个TableProcess
     *          key: source_table:operate_type  由table_process 的operator_type 和 sink_type 共同决定
     *          value: TableProcess
     *
     *
     *      问题：
     *      java -jar 产生数据 --> mysql --> maxwell到kafka
     *      maxwell-bootstrap 会全量导入旧表 --> kafak+hbase
     *
     *      1.业务数据: mysql->maxwell->kafka->flink
     *      2.动态表配置表的数据: mysql->flink-sql-cdc
     *      3.把动态表配置表做成广播流与业务数据进行connect, 从而实现动态控制业务数据的sink方向
     *
     *
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dynamicSplit(SingleOutputStreamOperator<JSONObject> dbStream,SingleOutputStreamOperator<TableProcess> processTableStream) {

        // 1. 把配置表流做成广播状态
        // 1.1 状态描述符:  key: sourceTable_operatorType
        final MapStateDescriptor<String, TableProcess> tableProcessStateDescriptor = new MapStateDescriptor<>(
                "tableProcessState",
                String.class,
                TableProcess.class);

        // 1.2 创建广播流
        final BroadcastStream<TableProcess> tableProcessBroadcastStream = processTableStream
                .broadcast(tableProcessStateDescriptor);

        // 2. 用数据流去关联广播流
        final SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> sinkToKafkaStream = dbStream
                .connect(tableProcessBroadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {

                    // 过滤掉不需要的字段    sinkColumns 是 tableProcess.getSinkColumns()
                    // 只保留表中配置的字段
                    private void filterColumns(JSONObject dataJsonObj, String sinkColumns) {

                        final List<String> columns = Arrays.asList(sinkColumns.split(","));
                        dataJsonObj
                                .entrySet()
                                .removeIf(entry -> !columns.contains(entry.getKey()));
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        // 处理数据流中的元素  connect之后在ctx中了
                        final ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx
                                .getBroadcastState(tableProcessStateDescriptor);

                        // 事实表数据存入主流(sink 到 Kafka),  维度表数据存入侧输出流(通过Phoenix sink到 HBase)
                        // 1. 获取表名,操作类型, 和数据
                        String tableName = jsonObj.getString("table");
                        String operateType = jsonObj.getString("type");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                        //1.1 如果是使用Maxwell的初始化功能，那么type类型为bootstrap-insert,我们这里也标记为insert，方便后续处理
                        if ("bootstrap-insert".equals(operateType)) {
                            operateType = "insert";
                            jsonObj.put("type", operateType);
                        }

                        // 2. 获取配置信息  需保证tableName和operateType均不包含特殊字符_
                        final String key = tableName + "_" + operateType;
                        if (broadcastState.contains(key)) {
                            // 根据key,用广播流HashMap进行get(key)，获取sinkType
                            final TableProcess tableProcess = broadcastState.get(key);

                            // 2.1 sink到Kafka或者HBase的时候, 不是所有字段都需要保存, 过滤掉不需要的
                            // 不需要返回值吗?
                            filterColumns(dataJsonObj, tableProcess.getSinkColumns());

                            // 2.2 开始分流: 事实表数据在主流, 维度表数据在侧输出流
                            final String sinkType = tableProcess.getSinkType();
                            if (TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(sinkType)) {
                                out.collect(Tuple2.of(jsonObj, tableProcess));
                            } else if (TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(sinkType)) {
                                ctx.output(hbaseTag, Tuple2.of(jsonObj, tableProcess));
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess tableProcess,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        System.out.println(tableProcess);

                        // 处理广播流中的元素
                        // 1. 获取广播状态, 其实存储的是一个map
                        final BroadcastState<String, TableProcess> broadcastState = ctx
                                .getBroadcastState(tableProcessStateDescriptor);

                        // 2. 以sourceTable作为Key 存入map集合中
                        broadcastState.put(tableProcess.getSourceTable() + "_" + tableProcess.getOperateType(), tableProcess);

                    }
                });

        return sinkToKafkaStream;

    }

}



