package com.atguigu.gmall.realtime.dwd.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2024/5/29
 * 该案例演示了侧输出流标签创建问题
 */
public class Test01_OutputTag {
    public static void main(String[] args) {
        //OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag");
        //OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag", TypeInformation.of(String.class));
    }
}
