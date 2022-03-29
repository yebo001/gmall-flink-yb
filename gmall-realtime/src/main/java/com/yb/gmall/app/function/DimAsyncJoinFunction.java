package com.yb.gmall.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 *
 */
public interface DimAsyncJoinFunction {

    /**
     * 处理关联的key
     * @param input
     * @return
     */
    String getKey(T input);

    /**
     * 处理维度表关联字段
     * @param input
     * @param dimInfo
     * @throws ParseException
     */
    void join(T input, JSONObject dimInfo) throws ParseException;


}
