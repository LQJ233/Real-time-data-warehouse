package com.atguigu.gmall.realtime.dws.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/6/09
 * 该案例演示了Flink读写Doris_SQL
 */
public class Test01_Doris_SQL {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L);
        //TODO 3.从指定的Doris表中读取数据
        /*tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode SMALLINT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                "    )   " +
                "    WITH (  " +
                "      'connector' = 'doris',  " +
                "      'fenodes' = 'hadoop102:7030',  " +
                "      'table.identifier' = 'test.table1',  " +
                "      'username' = 'root',  " +
                "      'password' = 'aaaaaa'  " +
                ")  ");
        // 读
        tEnv.sqlQuery("select * from flink_doris").execute().print();*/

        //TODO 4.向指定的Doris表中读取数据
        tEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")  ");

        tEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");


    }
}
