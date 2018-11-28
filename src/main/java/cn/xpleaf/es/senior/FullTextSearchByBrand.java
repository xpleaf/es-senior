package cn.xpleaf.es.senior;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author xpleaf
 * @date 2018/11/29 12:00 AM
 */
public class FullTextSearchByBrand {

    public static void main(String[] args) throws Exception {
        // settings
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        // client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // matchQuery
        System.out.println("----------matchQuery----------");
        SearchResponse searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.matchQuery("brand", "宝马"))
                .execute()
                .actionGet();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        // multiMatchQuery
        System.out.println("----------multiMatchQuery----------");
        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.multiMatchQuery("宝马", "brand", "name"))
                .execute()
                .actionGet();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        // termQuery
        System.out.println("----------termQuery----------");
        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.termQuery("name.raw", "宝马318"))
                .execute()
                .actionGet();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        // prefixQuery
        System.out.println("----------prefixQuery----------");
        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.prefixQuery("name", "宝"))
                .execute()
                .actionGet();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

}
