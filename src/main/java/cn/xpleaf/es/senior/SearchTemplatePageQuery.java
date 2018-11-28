package cn.xpleaf.es.senior;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.HashMap;

/**
 * @author xpleaf
 * @date 2018/11/28 10:39 PM
 */
public class SearchTemplatePageQuery {

    public static void main(String[] args) throws Exception {
        // setting
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        // client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        // scriptParams
        HashMap<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("from", 0);
        scriptParams.put("size", 1);
        scriptParams.put("brand", "宝马");

        // search
        SearchResponse searchResponse = new SearchTemplateRequestBuilder(client)
                .setScript("page_query_by_brand")
                .setScriptType(ScriptType.FILE)
                .setScriptParams(scriptParams)
                .setRequest(new SearchRequest("car_shop").types("sales"))
                .get()
                .getResponse();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

}
