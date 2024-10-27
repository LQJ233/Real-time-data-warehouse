package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.service.TrafficStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 流量域统计controller
 */
@RestController
public class TrafficStatsController {

    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvCt(date, limit);
        List chList = new ArrayList();
        List uvCtList = new ArrayList();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }


        String json = "{\"status\": 0,\"data\":{\"categories\": [\""+ StringUtils.join(chList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";

        return json;
    }
}
