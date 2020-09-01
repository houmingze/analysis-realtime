package com.atguigu.publisher.service.impl;

import com.atguigu.publisher.service.PublisherService;
import com.atguigu.publisher.util.DateUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/1 9:45
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder  = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Long result = 0L;
        String indexName = "gmall_dau_info_" + DateUtil.getESDate(date);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
            JsonObject jsonObject = searchResult.getJsonObject();
            JsonObject hits = jsonObject.getAsJsonObject("hits");
            JsonObject total = hits.getAsJsonObject("total");
            result = total.get("value").getAsLong();
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("es查询报错");
        }
        return result;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        String indexName = "gmall_dau_info_" + DateUtil.getESDate(date);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder aggBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("es查询报错");
        }
        Map<String,Long> rsMap = new HashMap<>();
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation termsAgg = aggregations.getTermsAggregation("groupby_hr");
        if(termsAgg!=null){
            List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                rsMap.put(bucket.getKey(),bucket.getCount());
            }
        }
        return rsMap;
    }
}
