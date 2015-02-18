package nosql.workshop.batch.elasticsearch;

import nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Job d'import des rues de towns_paysdeloire.csv vers ElasticSearch (/towns/town)
 */
public class ImportTowns {

    public static void main(String[] args) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(ImportTowns.class.getResourceAsStream("/csv/towns_paysdeloire.csv")));
             Client elasticSearchClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress(ES_DEFAULT_HOST, ES_DEFAULT_PORT));) {

            checkIndexExists("towns", elasticSearchClient);

            BulkRequestBuilder bulkRequest = elasticSearchClient.prepareBulk();

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> insertTown(line, bulkRequest, elasticSearchClient));

            BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
            System.out.print("Town Ok");
            dealWithFailures(bulkItemResponses);
        }

    }

    private static void insertTown(String line, BulkRequestBuilder bulkRequest, Client elasticSearchClient) {
        line = ElasticSearchBatchUtils.handleComma(line);
        String[] split = line.split(",");

        String townName = split[1].replaceAll("\"", "");

        // location
        Double longitude = Double.valueOf(split[6]);
        Double latitude = Double.valueOf(split[7]);
        Double[] coordinates = {longitude,latitude};

        // payload
        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("input", townName);
        payloadMap.put("output", townName);
        Map<String, Object> payload = new HashMap<>();
        payload.put("townName", townName);
        payload.put("location", coordinates);
        payloadMap.put("payload", payload);

        // global object
        Map<String, Object> map = new HashMap<String,Object>();
        map.put("townName", townName);
        map.put("location", coordinates);
        map.put("payloader", payloadMap);

        bulkRequest.add(elasticSearchClient.prepareIndex("towns", "town").setSource(map));

    }
}
