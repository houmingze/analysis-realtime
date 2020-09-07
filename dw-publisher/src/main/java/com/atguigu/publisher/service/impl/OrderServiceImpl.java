package com.atguigu.publisher.service.impl;

import com.atguigu.publisher.dao.OrderWideMapper;
import com.atguigu.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/7 19:16
 */
@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getTotalOrderAmount(String date) {
        return orderWideMapper.selectTotalOrderAmount(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String,BigDecimal> resultMap = new HashMap<>();
        List<Map<String, Object>> maps = orderWideMapper.selectOrderAmountHour(date);
        if(!CollectionUtils.isEmpty(maps)){
            for(Map<String,Object> map :maps){
                resultMap.put(map.get("hr").toString(),(BigDecimal)map.get("amount"));
            }
        }
        return resultMap;
    }
}
