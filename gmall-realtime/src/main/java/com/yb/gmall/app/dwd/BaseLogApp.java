package com.yb.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yb.gmall.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 主要任务
 *   1.识别新老用户：由于客户端业务新老用户识别不准确 这里采用状态的方式进行新老用户识别
 *   2.数据分流：将ODS中原始数据按照页面日志数据(主流)、启动日志数据(侧输出流)、曝光日志数据分流(侧输出流) 将分流数据写入Kafka不同Topic
 * 程序流程：
 *   1.logger.sh start
 *   2.java -jar gmall2020-mock-log-2020-12-18.jar
 *   3.BaseLogApp
 *   4.kafka-topics.sh --list --zookeeper hadoop102:2181/kafka
 */
//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)
//程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka
public class BaseLogApp {
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

        //TODO 2.消费 ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        //异常数据处理 将异常数据写入测输出流
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常 将数据写入测输出流
                    context.output(dirtyOutputTag, s);
                }

            }
        });

        //打印脏数据
        jsonObjDS.getSideOutput(dirtyOutputTag).print("Dirty>>>>>>>>>>");

        //TODO 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取数据中的"is_new"标记
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        //判断isNew标记是否为"1"
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                //修改isNew标记
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });

        //TODO 5.分流  侧输出流  页面：主流  启动：侧输出流  曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    collector.collect(jsonObject.toJSONString());

                    //取出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            //将输出写出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }

                }

            }

        });

        //TODO 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //TODO 7.将三个流进行打印并输出到对应的Kafka主题中
        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp");

    }
}
