package com.atguigu.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Felix
 * @date 2024/6/14
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}

