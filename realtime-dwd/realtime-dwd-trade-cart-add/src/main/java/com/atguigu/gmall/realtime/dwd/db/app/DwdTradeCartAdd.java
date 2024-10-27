package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/6/04
 * 加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );

    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db主题中读取数据  创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //TODO 过滤出加购数据  table='cart_info' type='insert' 、type = 'update' 并且修改的是加购商品的数量，修改后的值大于修改前的值
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `data`['id'] id,\n" +
                "   `data`['user_id'] user_id,\n" +
                "   `data`['sku_id'] sku_id,\n" +
                "   if(type='insert',`data`['sku_num'], CAST((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts\n" +
                "from topic_db \n" +
                "where `table`='cart_info' \n" +
                "and (\n" +
                "    type = 'insert'\n" +
                "    or\n" +
                "    (type='update' and `old`['sku_num'] is not null and (CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))\n" +
                ")");
        //cartInfo.execute().print();
        //TODO 将过滤出来的加购数据写到kafka主题中
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

    }
}
