package cn.edu.sysu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author : song bei chang
 * @create 2021/8/6 8:47
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class T3 {

    //窗口起始时间
    private String stt;
    //窗口结束时间
    private String edt;

    //sku名称
    private String keyword;

    //spu名称
    private String source;
    //品牌编号
    private Long ct;

    //统计时间戳
    private Long ts;

}



