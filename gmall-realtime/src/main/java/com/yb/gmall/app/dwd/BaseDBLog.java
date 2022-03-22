package com.yb.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.yb.gmall.app.function.CustomerDeserialization;
import com.yb.gmall.bean.TableProcess;
import com.yb.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

/**
 * 主要任务
 *  1.接收kafka数据 过滤空值数据
 *  2.实现动态分流功能，提供三种方式:
 *      （1）使用Zookeeper存储，通过Watch的方式监听
 *      （2）使用MySQL数据库存储，周期性同步
 *      （3）使用MySQL数据库存储，使用广播流（选择）
 *  3.业务数据保存到Kafka主题中，维度数据保存到HBase中
 * 程序流程：
 *  1.
 */
//数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
//程  序：           mockDb -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase,zk,hdfs)
public class BaseDBLog {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.消费Kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流 (实际项目中可以过滤空值脏数据)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String type = jsonObject.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //TODO 4.使用FlinkCDC消费配置表并处理成 广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())//首次启动时对被监控的数据库表进行初始快照，并继续读取最新的binlog。
                .deserializer(new CustomerDeserialization())//反序列化器
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        //定义一个MapStateDescriptor来描述我们要广播的数据的格式
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {};
//        connectedStream.process(new TableProcess(hbaseTag,mapStateDescriptor));

        //TODO 7.提取Kafka流数据和HBase流数据

        //TODO 8.将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表

        //TODO 9.启动任务
        env.execute("BaseDBLog");

    }
}
