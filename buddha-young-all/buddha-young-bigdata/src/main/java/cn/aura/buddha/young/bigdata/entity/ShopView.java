package cn.aura.buddha.young.bigdata.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShopView {
    private Integer shopId;
    private String cityName;
    private double perPay;
    private String timestamp;
    private long viewCount;
}
