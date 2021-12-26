package cn.edu.sysu.util;

import cn.edu.sysu.bean.NoSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * @Author : song bei chang
 * @create 2021/8/4 15:38
 *
 *      利用反射拼接sql
 */
public class MySinkUtil {

    /**
     * 到一个SinkFunction  sql插入语句只能是这样的:
     * insert into tableName(f1,f2,...)values(?,?,...)
     *
     * @param tableName
     * @param <T>
     * @return
     */
    public static <T> SinkFunction<T> getClickHouseSink(String db, String tableName, Class<T> tClass) {

        System.out.println("进入getClickHouseSink");
        String clickHouseUrl = "jdbc:clickhouse://bcc3:8123/" + db;
        String clickHourDriverName = "ru.yandex.clickhouse.ClickHouseDriver";

        // 根据表名拼接sql语句
        final Field[] fields = tClass.getDeclaredFields();
        final StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(tableName).append(" (");

        for (Field field : fields) {

            // 可能有些属性不需要写出去, 这样的字段我们用transient标识  如果这个字段有NoSink这个注解,则不要拼接
            if (!Modifier.isTransient(field.getModifiers())) {

                NoSink noSink = field.getAnnotation(NoSink.class);

                if (noSink == null) {
                    final String fieldName = field.getName();
                    sql.append(fieldName).append(",");
                }

            }
        }
        // 拼接的最后一个逗号去掉
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")values(");

        for (Field field : fields) {
            if (!Modifier.isTransient(field.getModifiers())) {

                NoSink noSink = field.getAnnotation(NoSink.class);
                if (noSink == null) {
                    sql.append("?,");
                }
            }
        }

        // 拼接的最后一个逗号去掉
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        System.out.println("SQL : "+sql.toString());
        return getJdbcSink(clickHouseUrl,
                clickHourDriverName,
                sql.toString());
    }

    private static <T> SinkFunction<T> getJdbcSink(String url,
                                                   String driver,
                                                   String sql) {

        return JdbcSink.<T>sink(sql,
                (ps, t) -> {
                    try {
                        final Field[] fields = t.getClass().getDeclaredFields();
                        for (int i = 0 , position = 1; i < fields.length; i++) {
                            if (!Modifier.isTransient(fields[i].getModifiers())) {

                                NoSink noSink = fields[i].getAnnotation(NoSink.class);
                                if (noSink == null) {
                                    fields[i].setAccessible(true);
                                    Object v = fields[i].get(t);
                                    ps.setObject(position++, v);
                                }

                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .build()
        );

    }


    /**
     * 测试 clickHouse 是否有效
     * 待优化
     */
    public static void main(String[] args) {

        System.out.println("进入getClickHouseSink");
        String clickHouseUrl = "jdbc:clickhouse://182.61.59.65:8123/gmall_realtime"  ;
        String clickHourDriverName = "ru.yandex.clickhouse.ClickHouseDriver";

        String sql = "insert into visitor_stats_2021 values('2021-05-16 16:18:00','2021-03-16 16:18:10','v2.1.134','oppo',440000,0,0,10,2,1,0,1615882682000)";

        getJdbcSink(clickHouseUrl,
                clickHourDriverName,
                sql);

    }
}


