package com.atguigu.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.publisher.service.PublisherService;
import com.atguigu.publisher.util.DateUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/1 9:44
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realTimgTotal(@RequestParam("date") String date) {
        List<Map<String, Object>> rsList = new ArrayList<>();

        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = publisherService.getDauTotal(date);
        dauMap.put("id", dauTotal == null ? 0L : dauTotal);
        rsList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        rsList.add(newMidMap);
        return JSON.toJSONString(rsList);
    }

    @GetMapping("realtime-hour")
    public String realTimeHour(@RequestParam("date") String date) {
        Map<String, Map<String, Long>> rsMap = new HashMap<>();
        Map<String, Long> tdMap = publisherService.getDauHour(date);
        rsMap.put("today", tdMap);
        Map<String, Long> ysMap = publisherService.getDauHour(DateUtil.getYD(date));
        rsMap.put("yesterday", ysMap);
        return JSON.toJSONString(rsMap);
    }

}
