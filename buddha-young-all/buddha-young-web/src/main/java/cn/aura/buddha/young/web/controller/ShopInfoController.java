package cn.aura.buddha.young.web.controller;

import cn.aura.buddha.young.bigdata.entity.ShopView;
import cn.aura.buddha.young.bigdata.sql.ShopViewAnalysis;
import cn.aura.buddha.young.web.entity.ShopInfo;
import cn.aura.buddha.young.web.service.ShopInfoService;
import org.apache.spark.sql.execution.columnar.MAP;
import org.json4s.jackson.Json;
import org.json4s.jackson.Json$;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class ShopInfoController {

    @Autowired
    ShopInfoService shopInfoService;

    @GetMapping("/shop/list")
    public List<ShopInfo> getShopInfoList() {
        return shopInfoService.getShopInfoList();
    }

    @GetMapping("/viewbytime/{shopId}/{type}")
    public ViewByTime search(@PathVariable Integer shopId, @PathVariable Integer type) {
       List<ShopView> list = ShopViewAnalysis.viewByTimeAnalysis(shopId, type);

       String[] dates = new String[list.size()];
       Long[] count = new Long[list.size()];

       for (int i = 0; i < list.size(); i++) {
           dates[i] = list.get(i).getTimestamp();
           count[i] = list.get(i).getViewCount();
       }

        ViewByTime viewByTime = new ViewByTime();
        viewByTime.setX(dates);
        viewByTime.setY(count);

       return viewByTime;
    }

    @GetMapping("/viewbytop50")
    public Collection<ViewByTop50> search() {
        List<ShopView> list = ShopViewAnalysis.viewByTop50Analysis();

        Map<String, ViewByTop50> map = new HashMap<>();

        for (ShopView sv : list) {
            if (map.containsKey(sv.getCityName())) {
                map.get(sv.getCityName()).getData().add(new Double[]{(double)sv.getViewCount(), sv.getPerPay()});
            } else {
                ViewByTop50 viewByTop50 = new ViewByTop50();
                viewByTop50.setName(sv.getCityName());

                List<Double[]> datas = new ArrayList<>();
                datas.add(new Double[]{(double)sv.getViewCount(), sv.getPerPay()});

                viewByTop50.setData(datas);

                map.put(sv.getCityName(), viewByTop50);
            }
        }

        return map.values();

    }

    class ViewByTime {
        private String[] x;
        private Long[] y;

        public String[] getX() {
            return x;
        }

        public void setX(String[] x) {
            this.x = x;
        }

        public Long[] getY() {
            return y;
        }

        public void setY(Long[] y) {
            this.y = y;
        }
    }

    class ViewByTop50 {
        private String name;
        private String type = "scatter";
        private List<Double[]> data;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<Double[]> getData() {
            return data;
        }

        public void setData(List<Double[]> data) {
            this.data = data;
        }
    }
}
