package com.atguigu.analysis.logger.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.analysis.logger.common.Response;
import com.atguigu.analysis.logger.service.LoggerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author ：hmz
 * @date ：Created in 2020/8/24 17:50
 */
@Slf4j
@RequestMapping("/logger")
@RestController
public class LoggerController {

    @Autowired
    LoggerService loggerService;

    @PostMapping("/applog")
    public Response appLog(@RequestBody JSONObject logJO){
        loggerService.saveLog(logJO);
        loggerService.sendKafka(logJO);
        return Response.success();
    }
}
