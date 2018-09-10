package cn.aura.buddha.young.web.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "shop_info")
public class ShopInfo implements Serializable {
    @Id
    private Integer shopId;
    @Column(name = "city_name")
    private String cityName;
    @Column(name = "location_id")
    private Integer locationId;
    @Column(name = "per_pay")
    private double perPay;
    @Column(name = "score")
    private double score;
    @Column(name = "comment_cnt")
    private Integer commentCnt;
    @Column(name = "shop_level")
    private Integer shopLevel;
    @Column(name = "cate_1_name")
    private String cate1Name;
    @Column(name = "cate_2_name")
    private String cate2Name;
    @Column(name = "cate_3_name")
    private String cate3Name;
}
