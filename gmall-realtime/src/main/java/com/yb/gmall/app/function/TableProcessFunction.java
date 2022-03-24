package com.yb.gmall.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yb.gmall.bean.TableProcess;
import com.yb.gmall.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 处理主流和广播流
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    /**
     * 建立phoenix链接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 处理从ods_base_db读取过来的数据 主流数据
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //2.过滤输出字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //将输出表/主题信息写入Value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //Kafka数据 输出到主流
                out.collect(value);
            } else if (tableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //HBase数据 输出到测输出流
                ctx.output(objectOutputTag, value);
            }

        } else {
            System.out.println("该组合Key：" + key + "不存在！");
        }

    }

    /**
     * 处理MySQL中配置表 广播流
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    //value:{"db":"","tn":"","before":{},"after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
                    );
        }

        //3.写状态 广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" +tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);

    }

    //建表语句 : create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String filed = fields[i];
                //判断是否为主键
                if (sinkPk.equals(filed)) {
                    createTableSQL.append(filed).append(" varchar primary key ");
                } else {
                    createTableSQL.append(filed).append(" varchar ");

                }

                //判断是否为最后一个字段 如果不是则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }

         }

            createTableSQL.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        }finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
     * @param sinkColumns id,tm_name
     *                    {"id":"11","tm_name":"atguigu"}
     */
    private static  void filterColumn(JSONObject data, String sinkColumns) {

        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

}
