package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author Felix
 * @date 2024/6/12
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
