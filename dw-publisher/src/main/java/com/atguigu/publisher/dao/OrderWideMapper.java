package com.atguigu.publisher.dao;

import org.apache.ibatis.annotations.Mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/7 19:20
 */
@Mapper
public interface OrderWideMapper {
    BigDecimal selectTotalOrderAmount(String date);

    List<Map<String,Object>> selectOrderAmountHour(String date);
}
