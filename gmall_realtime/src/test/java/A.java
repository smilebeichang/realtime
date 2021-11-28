import cn.edu.sysu.bean.OrderWide;
import com.alibaba.fastjson.JSON;

/**
 * @Author : song bei chang
 * @create 2021/8/3 10:50
 */
public class A {

    public static void main(String[] args) {
        String json = "{\"detail_id\":\"80819\",\"order_id\":\"27255\",\"sku_id\":\"23\",\"order_price\":\"40.00\",\"sku_num\":\"1\",\"sku_name\":\"十月稻田 辽河长粒香 东北大米 5kg\",\"province_id\":\"11\",\"order_status\":\"1001\",\"user_id\":\"2391\",\"total_amount\":\"2860.00\",\"activity_reduce_amount\":\"0.00\",\"coupon_reduce_amount\":\"0.00\",\"original_total_amount\":\"2845.00\",\"feight_fee\":\"15.00\",\"split_feight_fee\":\"null\",\"split_activity_amount\":\"null\",\"split_coupon_amount\":\"null\",\"split_total_amount\":\"40.00\",\"expire_time\":\"null\",\"create_time\":\"2021-07-27 16:46:07\",\"operate_time\":\"null\",\"create_date\":\"2021-07-27\",\"create_hour\":\"16\",\"province_name\":\"江西\",\"province_area_code\":\"360000\",\"province_iso_code\":\"CN-36\",\"province_3166_2_code\":\"CN-JX\",\"user_age\":\"52\",\"user_gender\":\"F\",\"spu_id\":\"7\",\"tm_id\":\"6\",\"category3_id\":\"803\",\"spu_name\":\"十月稻田 长粒香大米 东北大米 东北香米 5kg\",\"tm_name\":\"长粒香\",\"category3_name\":\"米面杂粮\"}";

        OrderWide s = JSON.parseObject(json, OrderWide.class);
        System.out.println(s);

    }
}



