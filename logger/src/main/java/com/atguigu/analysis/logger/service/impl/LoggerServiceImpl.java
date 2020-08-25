package com.atguigu.analysis.logger.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.analysis.logger.common.Constant;
import com.atguigu.analysis.logger.service.LoggerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author ：hmz
 * @date ：Created in 2020/8/24 17:51
 */
@Slf4j
@Service
public class LoggerServiceImpl implements LoggerService {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Override
    public void saveLog(JSONObject logJO) {
        log.info(logJO.toJSONString());
    }

    @Override
    public void sendKafka(JSONObject logJO) {
        String val = logJO.getString("start");
        if(val!=null&&val.length()>0){
            kafkaTemplate.send(Constant.KAFKA_TOPIC_START,logJO.toJSONString());
        }else{
            kafkaTemplate.send(Constant.KAFKA_TOPIC_EVENT,logJO.toJSONString());
        }
    }
}
