package cn.edu.sysu.util;


import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc工具类
 *
 * @Author lizhenchao
 * @Date 2021/2/4 21:07
 */
public class MyJDBCUtil {

    public static Connection getConnection(String url) {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> clazz,
                                        boolean underScoreToCamel) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            // 给sql中的占位符赋值
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            // 1. 获取结果集
            final ResultSet resultSet = ps.executeQuery();
            // 2. 获取元数据: 比如列名啥的  通过结果查询元数据
            final ResultSetMetaData metaData = resultSet.getMetaData();
            // 3. 用于返回最终结果
            final ArrayList<T> result = new ArrayList<>();
            // 4. 遍历每一行数据
            while (resultSet.next()) {
                final T obj = clazz.newInstance();
                // 5. 获取每一列
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    // 5.1 获取列名
                    String columnName = metaData.getColumnName(i + 1);
                    // 5.2 下划线转换成驼峰命名
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    // 5.3 使用apache的BeanUtils工具
                    BeanUtils.setProperty(obj, columnName, resultSet.getObject(i + 1));
                }
                // 6. 把对象添加结果集合中
                result.add(obj);
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("jdbc查询失败");
        }

    }
}



