package com.atguigu.analysis.logger.service;

import com.alibaba.fastjson.JSONObject;

/**
 * @author ：hmz
 * @date ：Created in 2020/8/24 17:50
 */
public interface LoggerService
{
    void saveLog(JSONObject logJO);

    void sendKafka(JSONObject logJO);
}
