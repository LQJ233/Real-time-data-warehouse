package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @author Felix
 * @date 2024/5/28
 * 处理主流业务数据和广播流配置数据关联后的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    //处理主流业务数据               根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
        //获取处理的数据的表名
        String table = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
        TableProcessDim tableProcessDim = null ;

        if((tableProcessDim =  broadcastState.get(table)) != null
                || (tableProcessDim =  configMap.get(table)) != null){
            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据

            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);

            //在向下游传递数据前，补充对维度数据的操作类型属性
            String type = jsonObj.getString("type");
            dataJsonObj.put("type",type);

            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    //处理广播流配置信息    将配置数据放到广播状态中或者从广播状态中删除对应的配置  k:维度表名     v:一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
        //获取对配置表进行的操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取维度表名称
        String sourceTable = tp.getSourceTable();
        if("d".equals(op)){
            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        }else{
            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp);
        }
    }

    //过滤掉不需要传递的字段
    //dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
    //sinkColumns  id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
}
