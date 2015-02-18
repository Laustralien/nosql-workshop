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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
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

        CompletionSuggestionBuilder compBuilder = new CompletionSuggestionBuilder("complete");
        compBuilder.text(searchQuery);
        compBuilder.field("nom");

        SearchResponse resp = elasticSearchClient
                .prepareSearch("installations")
                .setTypes("installation")
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSuggestion(compBuilder)
                .execute()
                .actionGet();

        SearchHit[] installations = resp.getHits().getHits();
        List<Installation> ret = new ArrayList<>();
        for (SearchHit sh : installations){
            Installation i = this.mapToInstallation(sh);
            ret.add(i);
        }

        return ret;
    }

    public List<TownSuggest> suggestTownName(String townName){

        CompletionSuggestionBuilder compBuilder = new CompletionSuggestionBuilder("towns");
        compBuilder.text(townName);
        compBuilder.field("townName");

        SearchResponse response = elasticSearchClient.prepareSearch("towns")
                .setTypes("completions")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSuggestion(compBuilder)
                .execute()
                .actionGet();

        CompletionSuggestion compSuggestion = response.getSuggest().getSuggestion("towns");
        List<TownSuggest> suggestions=  new ArrayList<>();
        System.out.println(compSuggestion.getName());
        List<CompletionSuggestion.Entry.Option> opts = compSuggestion.iterator().next().getOptions();

        for(CompletionSuggestion.Entry.Option o : opts) {
            try {
                suggestions.add(objectMapper.readValue(o.getPayloadAsString(), TownSuggest.class));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return suggestions;
    }

    public Double[] getTownLocation(String townName)  {
        SearchResponse response = elasticSearchClient.prepareSearch("towns")
                .setTypes("town")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
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
        return new Double[2];
    }
}
