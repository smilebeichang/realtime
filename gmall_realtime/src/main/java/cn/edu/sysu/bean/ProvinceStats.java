package cn.edu.sysu.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/8/6 15:37
 */
@Data
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts;
    
}
