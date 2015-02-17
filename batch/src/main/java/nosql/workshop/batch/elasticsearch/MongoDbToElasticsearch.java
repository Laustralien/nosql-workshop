package nosql.workshop.batch.elasticsearch;

import com.mongodb.*;
import nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.UnknownHostException;

import static nosql.workshop.batch.elasticsearch.util.ElasticSearchBatchUtils.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Transferts les documents depuis MongoDB vers Elasticsearch.
 */
public class MongoDbToElasticsearch {

    public static void main(String[] args) throws UnknownHostException {

        MongoClient mongoClient = null;

        long startTime = System.currentTimeMillis();
        try (Client elasticSearchClient =
                     new TransportClient()
                             .addTransportAddress(new InetSocketTransportAddress(ES_DEFAULT_HOST, ES_DEFAULT_PORT));){
            
            checkIndexExists("installations", elasticSearchClient);

            mongoClient = new MongoClient();

            // cursor all database objects from mongo db
            DBCursor cursor = ElasticSearchBatchUtils.getMongoCursorToAllInstallations(mongoClient);
            BulkRequestBuilder bulkRequest = elasticSearchClient.prepareBulk();

            while (cursor.hasNext()) {
                DBObject object = cursor.next();
                String objectId = (String) object.get("_id");
                object.removeField("dateMiseAJourFiche");

/*
                adresseObject.put("numero", columns[6]);
                adresseObject.put("voie", columns[7]);
                adresseObject.put("lieuDit", columns[5]);
                adresseObject.put("codePostal",columns[4] );
                adresseObject.put("commune", columns[2]);
                dbObject.put("adresse", adresseObject);
                locationObject.put("type", "Point");
                locationObject.put("coordinates", coord);
                dbObject.put("location",locationObject);
*/

                // TODO codez l'écriture du document dans ES
                try {
                    bulkRequest.add(elasticSearchClient.prepareIndex("nosql-workshop", "installations", objectId)
                        .setSource(jsonBuilder()
                            .startObject()
                            .field("nom", object.get("nom"))
                            .field("multiCommune", object.get("multiCommune"))
                            .field("nbPlacesParking", object.get("nbPlacesParking"))
                            .field("nbPlacesParkingHandicapes", object.get("nbPlacesParkingHandicapes"))
                            .field("adresse", object.get("adresse"))
                            .field("location", object.get("location"))
                            .endObject()
                        )
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();

            dealWithFailures(bulkItemResponses);

            System.out.println("Inserted all documents in " + (System.currentTimeMillis() - startTime) + " ms");
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }


    }

}
