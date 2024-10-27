package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/5/27
 * 该案例演示了FlinkCDC的使用
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);
        //TODO 3.使用FlinkCDC读取MySQL表中数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config") // set captured database
                .tableList("gmall2024_config.t_user") // set captured table
                .username("root")
                .password("123456")
                //.startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //"op":"r": {"before":null,"after":{"id":1,"name":"zs","age":20},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1716783196484,"transaction":null}
        //"op":"c": {"before":null,"after":{"id":3,"name":"ww","age":40},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716783302000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":394,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1716783301977,"transaction":null}
        //"op":"u"：{"before":{"id":3,"name":"ww","age":40},"after":{"id":3,"name":"wwww","age":40},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716783336000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":719,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1716783335313,"transaction":null}
        //"op":"d"：{"before":{"id":3,"name":"wwww","age":40},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1716783372000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":1051,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1716783371258,"transaction":null}


        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print(); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
