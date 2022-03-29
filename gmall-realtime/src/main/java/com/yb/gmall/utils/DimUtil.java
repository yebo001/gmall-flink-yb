package com.yb.gmall.utils;

import com.alibaba.fastjson.JSONObject;
import com.yb.gmall.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        //查询Phoenix之前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        //DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //若redis中没有 则去phoenix中查询维度数据
        //拼接查询语句
        //select * from db.tn where id='18';
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName +
                " where id='" + id + "'";

        //查询Phoenix




    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

}
