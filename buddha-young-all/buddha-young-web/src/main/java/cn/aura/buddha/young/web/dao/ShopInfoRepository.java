package cn.aura.buddha.young.web.dao;

import cn.aura.buddha.young.web.entity.ShopInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ShopInfoRepository extends JpaRepository<ShopInfo, Integer> {

}
