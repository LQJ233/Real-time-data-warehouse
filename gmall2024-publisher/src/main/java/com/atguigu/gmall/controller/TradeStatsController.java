package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 交易域统计Controller
 */
@RestController
public class TradeStatsController {

    @Autowired
    private TradeStatsService tradeStatsService;
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            //说明请求的时候，没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\n" +
                "\t\t  \"status\": 0,\n" +
                "\t\t  \"data\": "+gmv+"\n" +
                "\t\t}";
        return json;
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}
