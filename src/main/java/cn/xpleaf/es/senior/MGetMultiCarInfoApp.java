package cn.xpleaf.es.senior;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author xpleaf
 * @date 2018/11/28 2:44 PM
 */
public class MGetMultiCarInfoApp {

    public static void main(String[] args) throws Exception {
        // 配置
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        // 构建client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // get
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("car_shop", "cars", "1")
                .add("car_shop", "cars", "2")
                .get();

        // foreach since jdk 1.8
        multiGetResponse.forEach(multiGetItemResponse -> {
            GetResponse response = multiGetItemResponse.getResponse();
            if(response.isExists()) {
                System.out.println(response.getSourceAsString());
            }
        });

        // foreach
        for(MultiGetItemResponse multiGetItemResponse : multiGetResponse) {
            GetResponse response = multiGetItemResponse.getResponse();
            if(response.isExists()) {
                System.out.println(response.getSourceAsString());
            }
        }

    }

}
