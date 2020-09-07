package com.atguigu.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/7 19:16
 */
public interface OrderService {
    BigDecimal getTotalOrderAmount(String date);

    Map<String, BigDecimal> getOrderAmountHour(String yd);
}
