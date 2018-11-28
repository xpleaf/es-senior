package cn.xpleaf.es.senior;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author xpleaf
 * @date 2018/11/28 8:11 PM
 */
public class BulkUploadSalesDataApp {

    public static void main(String[] args) throws Exception {
        // 配置
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        // client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // 批量写入requestBuilder
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        // indexRequestBuilder
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop", "sales", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("brand", "奔驰")
                            .field("name", "奔驰C200")
                            .field("price", 350000)
                            .field("produce_date", "2017-01-20")
                            .field("sale_date", "2017-01-25")
                        .endObject());
        bulkRequestBuilder.add(indexRequestBuilder);

        // updateRequestBuilder
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop", "sales", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("sale_price", 29000)
                        .endObject());
        bulkRequestBuilder.add(updateRequestBuilder);

        // deleteRequestBuilder
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop", "sales", "2");
        bulkRequestBuilder.add(deleteRequestBuilder);

        BulkResponse bulkResponse = bulkRequestBuilder.get();

        // 遍历结果
        for(BulkItemResponse bulkItemResponse : bulkResponse) {
            System.out.println(bulkItemResponse.status());
        }


    }

}
