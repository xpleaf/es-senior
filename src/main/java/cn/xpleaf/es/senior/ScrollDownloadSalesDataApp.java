package cn.xpleaf.es.senior;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author xpleaf
 * @date 2018/11/28 10:02 PM
 */
public class ScrollDownloadSalesDataApp {

    public static void main(String[] args) throws Exception {
        // setting
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        // client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));


        // search，设置为scroll search，并获取第一批次的数据
        SearchResponse searchResponse = client.prepareSearch("car_shop")
                .setTypes("sales")
                .setQuery(QueryBuilders.termQuery("brand.keyword", "宝马"))
                .setScroll(new TimeValue(60000))    // 设置时间窗口为60s，即让es保存这个scroll的句柄为1分钟
                .setSize(1)     // 每批次的数据大小
                .get();

        int batchCount = 0;
        do {
            batchCount++;
            System.out.println("batch: " + batchCount);
            for(SearchHit searchHit : searchResponse.getHits().getHits()) {
                System.out.println(searchHit.getSourceAsString());

                // 每次查询一批数据，比如1000行，然后写入本地的一个excel文件中

                // 如果说你一下子查询几十万条数据，不现实，jvm内存可能都会爆掉
            }

            // 获取下一批次的数据
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        } while (searchResponse.getHits().getHits().length != 0);

        client.close();
    }

}
