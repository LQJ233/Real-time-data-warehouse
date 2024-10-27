package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 交易域统计mapper接口
 */
public interface TradeStatsMapper {
    //获取某天总交易额
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    //获取某天各个省份交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date} " +
            " GROUP BY province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);

}
