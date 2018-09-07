package cn.aura.buddha.young.bigdata.util;

import redis.clients.jedis.Jedis;

public class RedisUtil {

    /*private static Jedis jedis = null;

    public static Jedis getJedis() {

        if (jedis == null) {
            jedis = new Jedis("172.16.186.128", 6379);
        }

        return jedis;
    }*/

    public static Jedis getJedis() {
        return new Jedis("172.16.186.128", 6379);
    }
}
