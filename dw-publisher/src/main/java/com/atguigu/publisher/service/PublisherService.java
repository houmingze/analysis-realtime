package com.atguigu.publisher.service;

import java.util.Map;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/1 9:44
 */
public interface PublisherService {
    Long getDauTotal(String date);

    Map<String, Long> getDauHour(String date);
}
