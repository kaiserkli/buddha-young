package cn.aura.buddha.young.web.controller;

import cn.aura.buddha.young.bigdata.entity.ShopView;
import cn.aura.buddha.young.web.entity.ShopInfo;
import cn.aura.buddha.young.web.service.ShopInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;

@Controller
public class ShopInfoController {

    @Autowired
    ShopInfoService shopInfoService;

    @GetMapping("/shop/list")
    public List<ShopInfo> getShopInfoList() {
        return shopInfoService.getShopInfoList();
    }

    @GetMapping("/viewbytime/{shopId}/{type}")
    public List<ShopView> search(@PathVariable Integer shopId, @PathVariable Integer type) {
       List<ShopView> list = ShopViewAnalysis.viewByTimeAnalysis(shopId, type);
       return list;
    }
}
