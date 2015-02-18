package nosql.workshop.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Created by Chris on 12/02/15.
 */
public class SearchService {
    public static final String INSTALLATIONS_INDEX = "installations";
    public static final String INSTALLATION_TYPE = "installation";
    public static final String TOWNS_INDEX = "towns";
    private static final String TOWN_TYPE = "town";


    public static final String ES_HOST = "es.host";
    public static final String ES_TRANSPORT_PORT = "es.transport.port";

    final Client elasticSearchClient;
    final ObjectMapper objectMapper;

    @Inject
    public SearchService(@Named(ES_HOST) String host, @Named(ES_TRANSPORT_PORT) int transportPort) {
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).build();
        elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, transportPort));

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Transforme un résultat de recherche ES en objet installation.
     *
     * @param searchHit l'objet ES.
     * @return l'installation.
     */
    private Installation mapToInstallation(SearchHit searchHit) {
        try {
            return objectMapper.readValue(searchHit.getSourceAsString(), Installation.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Recherche les installations à l'aide d'une requête full-text
     * @param searchQuery la requête
     * @return la listes de installations
     */
    public List<Installation> search(String searchQuery) {


        SearchResponse response = elasticSearchClient.prepareSearch("installations")
                .setTypes("installation")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.queryString(searchQuery)))
                .setExplain(true)
                .execute()
                .actionGet();

        List<Installation> installations = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit sh : hits){
            installations.add(mapToInstallation(sh));
        }

        return installations;
    }

    public List<TownSuggest> suggestTownName(String townName){

        CompletionSuggestionBuilder compBuilder = new CompletionSuggestionBuilder("towns");
        compBuilder.text(townName);
        compBuilder.field("townNameSuggest");

        SearchResponse searchResponse = elasticSearchClient.prepareSearch("towns")
                .setTypes("completion")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSuggestion(compBuilder)
                .execute().actionGet();

        CompletionSuggestion compSuggestion = searchResponse.getSuggest().getSuggestion("towns");

        List<TownSuggest> suggestions =  new ArrayList<>();
        List<CompletionSuggestion.Entry.Option> opts = compSuggestion.iterator().next().getOptions();

        for (CompletionSuggestion.Entry.Option opt : opts){
            try {
                suggestions.add(objectMapper.readValue(opt.getPayloadAsString(), TownSuggest.class));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return suggestions;
    }

    public Double[] getTownLocation(String townName)  {

        SearchResponse response = elasticSearchClient.prepareSearch("towns")
                .setTypes("town")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .addField("location")
                .setQuery(QueryBuilders.matchQuery("townName", townName))
                .setExplain(true)
                .execute()
                .actionGet();

        SearchHit[] hits = response.getHits().getHits();
        if(hits.length != 0){
            List<Object> values = hits[0].field("location").values();

            Double[] ret = new Double[values.size()];
            for(int i =0; i<values.size();i++){
                ret[i] = (Double) values.get(i);
            }
            return ret;

        }
        return null;
    }
}
