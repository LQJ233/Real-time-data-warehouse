package com.atguigu.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Felix
 * @date 2024/6/14
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
