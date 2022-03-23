package com.yb.gmall.app.function;

import com.alibaba.fastjson.JSONObject;
import com.yb.gmall.common.GmallConfig;
import com.yb.gmall.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 处理流程
 *  1.open初始化链接 phoenix
 *  2.invoke每条数据执行写入hbase操作
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    /**
     * 每条记录调用此方法 写入接收器
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable,
                    after);
            System.out.println(upsertSql);

            //预编译SQL
            connection.prepareStatement(upsertSql);

            //判断如果当前数据为更新操作,则先删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }

            //提交
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //data:{"tm_name":"Atguigu","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        //keySet.mkString(",");  =>  "id,tm_name"
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ")"
                + "values('" + StringUtils.join(values,"','") + "')";
    }


}
