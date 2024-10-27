package com.atguigu.gmall.realtime.dwd.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/5/30
 * 该案例演示了IntervalJoin的使用
 */
public class Test01_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从指定的网络端口读取员工数据
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("hadoop102", 8888)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Emp>() {
                                            @Override
                                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                                return emp.getTs();
                                            }
                                        }
                                )
                );
        //从指定的网络端口读取部门数据
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("hadoop102", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Dept>() {
                                            @Override
                                            public long extractTimestamp(Dept dept, long recordTimestamp) {
                                                return dept.getTs();
                                            }
                                        }
                                )
                );

        //使用IntervalJoin进行关联
        //底层： connect + 状态
        //判断是否迟到
        //将当前元素添加到缓存中
        //用当前元素和另外一条流中缓存数据进行关联
        //清缓存
        empDS
                .keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5),Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp,Dept>>() {
                            @Override
                            public void processElement(Emp emp, Dept dept, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                                out.collect(Tuple2.of(emp,dept));
                            }
                        }
                ).print();

        env.execute();
    }
}
