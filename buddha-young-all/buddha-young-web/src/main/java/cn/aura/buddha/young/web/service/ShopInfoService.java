package cn.aura.buddha.young.web.service;

import cn.aura.buddha.young.web.dao.ShopInfoRepository;
import cn.aura.buddha.young.web.entity.ShopInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ShopInfoService {

    @Autowired
    ShopInfoRepository shopInfoRepository;

    public List<ShopInfo> getShopInfoList() {
        return shopInfoRepository.findAll();
    }
}
