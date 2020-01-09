package pers.wilson.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static pers.wilson.elasticsearch.transport.TransportClientUtil.getTransportClient;

@Slf4j
public class Application {

    private static String INDEX_NAME = "security_log_2020.02.06";
    private static String INDEX_TYPE = "log";

    private static final TransportClient client;

    static {
        client = getTransportClient();
    }

    public static void main(String[] args) {

        log.info("Application start ...");
        String instruction = args.length > 0 ? args[0] : "";
        log.info("输入指令：{}", instruction);

        switch (instruction) {
//            增
            case "0":
                insert();
                break;
//            删
            case "1":
                delete(args[1]);
                break;
//            改
            case "2":
                break;
//            查
            case "3":
                query();
                break;
            default:
                break;
        }
    }

    public static void insert() {
        log.info("insert start ...");
        List<Map<String, Object>> collect = Stream.of(
                new HashMap<String, Object>(){
                    {
                        put("id", "1000");
                        put("create_time", System.currentTimeMillis());
                    }
                }).collect(Collectors.toList());
        for (Map<String, Object> map : collect) {
            client.prepareIndex(INDEX_NAME, INDEX_TYPE).setSource(map).get();
        }
        log.info("insert end .");
    }

    public static void delete(String id) {
        log.info("delete start ...");
        client.prepareDelete(INDEX_NAME, INDEX_TYPE, id).get();
        log.info("delete start .");
    }

    public static void query() {
        log.info("query start ...");

        SearchRequestBuilder requestBuilder = client.prepareSearch(INDEX_NAME)
                .setTypes(INDEX_TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(10000).setExplain(true);

        log.info("查询ES请求参数：{}", requestBuilder.toString());

        SearchResponse response = requestBuilder.execute().actionGet();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            log.info("第{}条：{}", i, response.getHits().getHits()[i].getSourceAsString());
        }
        Application.log.info("query end .");
    }
}
