package com.yb.gmall.app.function;

import com.alibaba.fastjson.JSONObject;
import com.yb.gmall.common.GmallConfig;
import com.yb.gmall.utils.DimUtil;
import com.yb.gmall.utils.ThreadPoolUtil;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.async.ResultFuture;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 处理异步I/O
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncJoinFunction{

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;//需要查询的维度表名

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPool();

    }

    /**
     *异步流触发操作
     * @param input
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(//提交任务
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //获取查询的主键
                            String id = getKey(input);

                            //查询维度信息
                            JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                            //补充维度信息
                            if ( dimInfo != null) {
                                join(input, dimInfo);
                            }

                            //将数据输出
                            resultFuture.complete(Collections.singletonList(input));

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    }
                }
        );
    }

    /**
     * 超时操作
     * @param input
     * @param resultFuture
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
        System.out.println("TimeOut:" + input);
    }
}
