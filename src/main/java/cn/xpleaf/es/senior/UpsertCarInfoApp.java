package cn.xpleaf.es.senior;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/11/28 10:59 AM
 */
public class UpsertCarInfoApp {

    public static void main(String[] args) throws Exception {
        // 配置
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", false)    // 是否开启自动探查es节点
                // 注意，如果只有一个节点，其本身就为master节点，那么设置了client.transport.sniff
                // 因为其是不会向master节点发送请求的，所以此时操作就会失败，所以在一个节点的情况下，可以把其设置为false
                .build();

        // client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // 创建索引请求
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", "1")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("brand", "宝马")
                            .field("name", "宝马320Li")
                            .field("price", 310000)
                            .field("price_date", "2017-01-01")
                        .endObject());

        // 带有upsert的update请求
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "1")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("price", 320000)
                        .endObject())
                .upsert(indexRequest);

        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.getVersion());

        GetResponse getResponse = client.prepareGet("car_shop", "cars", "1").get();
        System.out.println(getResponse.getSourceAsString());

    }

}
