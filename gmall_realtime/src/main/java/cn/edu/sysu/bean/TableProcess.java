package cn.edu.sysu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : song bei chang
 * @create 2021/11/27 22:46
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {

    /** 动态分流Sink常量 */
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    /** 来源表 */
    private String sourceTable;

    /** 操作类型 insert,update,delete */
    private String operateType;

    /** 输出类型 hbase kafka */
    private String sinkType;

    /** 输出表(主题) */
    private String sinkTable;

    /** 输出字段 */
    private String sinkColumns;

    /** 主键字段 */
    private String sinkPk;

    /** 建表扩展 */
    private String sinkExtend;

}


