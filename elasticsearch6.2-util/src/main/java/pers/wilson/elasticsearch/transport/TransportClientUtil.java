package pers.wilson.elasticsearch.transport;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static pers.wilson.elasticsearch.transport.TransportClientConfiguration.CLIENT;
import static pers.wilson.elasticsearch.transport.TransportClientConfiguration.getTransportClient;

@Slf4j
public class TransportClientUtil {

    public static void insert(String indexName, String indexType) {
        log.info("insert start ...");
        TransportClient client = getTransportClient();
        List<Map<String, Object>> collect = Stream.of(
                new HashMap<String, Object>(){
                    {
                        put("id", "1000");
                        put("create_time", System.currentTimeMillis());
                    }
                }).collect(Collectors.toList());
        for (Map<String, Object> map : collect) {
            client.prepareIndex(indexName, indexType).setSource(map).get();
        }
        client.close();
        log.info("insert end .");
    }

    public static void delete(String indexName, String indexType, String id) {
        log.info("delete start ...");
        TransportClient client = getTransportClient();
        client.prepareDelete(indexName, indexType, id).get();
        client.close();
        log.info("delete end .");
    }

    public static void update(String indexName, String indexType, String id) {
        log.info("update start ...");
        try {
            CLIENT.prepareUpdate(indexName, indexType, id)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("create_time", System.currentTimeMillis())
                            .endObject())
                    .get();
        } catch (IOException e) {
            log.error("update err.", e);
        }
        log.info("update end .");
    }

    public static void query(String indexName, String indexType) {
        log.info("query start ...");
        TransportClient client = getTransportClient();
        SearchRequestBuilder requestBuilder = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(10000).setExplain(true);

        log.info("查询ES请求参数：{}", requestBuilder.toString());

        SearchResponse response = requestBuilder.execute().actionGet();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            log.info("第{}条：{}", i, response.getHits().getHits()[i].getSourceAsString());
        }
        client.close();
        log.info("query end .");
    }
}
