package com.yb.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;

public class DimUtil {

//    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
//        //查询Phoenix之前先查询Redis
//    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

}
