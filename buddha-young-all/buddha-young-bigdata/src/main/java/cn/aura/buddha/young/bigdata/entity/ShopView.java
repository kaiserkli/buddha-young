package cn.aura.buddha.young.bigdata.entity;

public class ShopView {
    private Integer shopId;
    private String cityName;
    private double perPay;
    private String timestamp;
    private long viewCount;

    public Integer getShopId() {
        return shopId;
    }

    public void setShopId(Integer shopId) {
        this.shopId = shopId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public double getPerPay() {
        return perPay;
    }

    public void setPerPay(double perPay) {
        this.perPay = perPay;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }
}
