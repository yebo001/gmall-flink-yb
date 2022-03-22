package com.yb.gmall.app.dwd;

/**
 * 主要任务
 *  1.接收kafka数据 过滤空值数据
 *  2.实现动态分流功能，提供三种方式:
 *      （1）使用Zookeeper存储，通过Watch的方式监听
 *      （2）使用MySQL数据库存储，周期性同步
 *      （3）使用MySQL数据库存储，使用广播流（选择）
 *  3.业务数据保存到Kafka主题中，维度数据保存到HBase中
 */
//数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
//程  序：           mockDb -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase,zk,hdfs)
public class BaseDBLog {
    public static void main(String[] args) {

        //TODO 1.获取执行环境

        //TODO 2.消费Kafka ods_base_db 主题数据创建流

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流

        //TODO 4.使用FlinkCDC消费配置表并处理成         广播流

        //TODO 5.连接主流和广播流

        //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)

        //TODO 7.提取Kafka流数据和HBase流数据

        //TODO 8.将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表

        //TODO 9.启动任务

    }
}
