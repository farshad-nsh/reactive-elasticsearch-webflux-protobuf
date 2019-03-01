package com.farshad.reactive.elastic.provider;

import com.farshad.reactive.elastic.generated.domain.BondProtos;
import com.farshad.reactive.elastic.securities.bond.Bond;
import com.farshad.reactive.elastic.securities.bond.BondRepository;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


/**
 * @author farshad
 * @version 1.0.0
 */

@Component
public class ElasticsearchBondProvider implements BondRepository {

    private final RestHighLevelClient restHighLevelClient;


    public ElasticsearchBondProvider(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }


    /**
     * using webflux throughput calculated using jmitter =780 transaction/sec
     * @param bond
     * @return
     */
    @Override
    public Mono<Void> addDocument(Bond bond) {

        IndexRequest indexRequest = new IndexRequest("security", "bond")
                .source("title", bond.getTitle(),
                        "price", bond.getPrice());

/*
        String jsonString = "{" +
                "\"title\":\"ddd\"," +
                "\"price\":\"45\"," +
                "}";
        indexRequest.source(jsonString, XContentType.JSON);
*/

        System.out.println("bond.getPrice()="+bond.getPrice());
        System.out.println("bond.getTitle()="+bond.getTitle());

        return Mono.create(sink -> {
            ActionListener<IndexResponse> actionListener = new ActionListener<IndexResponse>() {

                @Override
                public void onResponse(IndexResponse indexResponse) {
                    System.out.println("succeeded at addDocument:"+"type:"+indexResponse.getType()+" index:"+indexResponse.getIndex());
                    sink.success();
                }
                @Override
                public void onFailure(Exception e) {
                    System.out.println("failed at addDocument");
                }
            };
            restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, actionListener);
        });
    }


    /**
     * without using webflux throughput calculated using jmitter =360 transaction/sec
     * @param bond
     * @return
     * @throws IOException
     */
    @Override
    public Bond addDocument2(Bond bond) throws IOException {
        IndexRequest indexRequest = new IndexRequest("security", "bond")
                .source("title", bond.getTitle(),
                        "price", bond.getPrice());
                restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT) ;
        return bond;
    }


    /**
     * using webflux jmitter result=
     * @return
     */
    @Override
    public Flux<Bond> findAll() {
        System.out.println("finding-----");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
         searchSourceBuilder.query(QueryBuilders.matchAllQuery());
         searchSourceBuilder.from(0);
         searchSourceBuilder.size(5000);  //show up to 200 bonds
        // searchSourceBuilder.query(QueryBuilders.typeQuery("bond"));
       // searchSourceBuilder.query(QueryBuilders.matchQuery("title","bondfarshad"));
        return getBondFlux(searchSourceBuilder);
    }

    /**
     * without using webflux result of jmitter=
     * @return
     * @throws IOException
     */
    @Override
    public List<Bond> findAll2() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(200);  //show up to 200 bonds
        //searchSourceBuilder.query(QueryBuilders.typeQuery("bond"));
        return getBondFlux2(searchSourceBuilder);
    }

    @Override
    public Flux<Bond> searchTermQueryByTitle(String title) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termQuery("title", title));
        return getBondFlux(searchSourceBuilder);
    }

    @Override
    public Flux<Bond> searchMatchPhraseQueryByTitle(String title) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("title", title));
        return getBondFlux(searchSourceBuilder);
    }


    private Flux<Bond> getBondFlux(SearchSourceBuilder searchSourceBuilder) {



        SearchRequest searchRequest = new SearchRequest("security");
        searchRequest.source(searchSourceBuilder);

        return Flux.<Bond>create(sink -> {
            ActionListener<SearchResponse> actionListener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {

                    for(SearchHit hit : searchResponse.getHits()) {
                        ObjectMapper objectMapper = new ObjectMapper();


                        //objectMapper.registerModule(new ProtobufModule());

                        try {
                            System.out.println("trying---------");
                            Bond bond = objectMapper.readValue(hit.getSourceAsString(), Bond.class);

                            sink.next(bond);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    sink.complete();
                }

                @Override
                public void onFailure(Exception e) {
                    System.out.println("---------failed at getBondFlux---------");
                }
            };

            restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener);

        });
    }


    private List<Bond> getBondFlux2(SearchSourceBuilder searchSourceBuilder) throws IOException {

        SearchRequest searchRequest = new SearchRequest("security");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse=restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);


          List<Bond> bondList=new ArrayList<>();


        for(SearchHit hit : searchResponse.getHits()) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                System.out.println("trying syncronously---------");
                Bond bond = objectMapper.readValue(hit.getSourceAsString(), Bond.class);
                bondList.add(bond);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

      /*
        val hits = restHighLevelClient.search(searchQuery, RequestOptions.DEFAULT).getHits().getHits();
        return Arrays.stream(hits).map(SearchHit::getSourceAsString).map(ElasticRepo::toListingsData).collect(Collectors.toList());
        */
        return bondList;
    }

    @Override
    public Mono<Bond> findById(String id) {

        GetRequest getRequest = new GetRequest(
                "security",
                "bond",
                id);

        //TODO: 구현
        return null;
    }

    @Override
    public Flux<Bond> deleteByIndex(String index) {

        //example index=security
        DeleteRequest deleteRequest=new DeleteRequest(index);

        return Flux.<Bond>create(sink -> {
            ActionListener<DeleteResponse> actionListener = new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    System.out.println("deleted successfully!");
                     sink.complete();
                }
                @Override
                public void onFailure(Exception e) {
                    System.out.println("---------failed at delete--------");
                }
            };
            restHighLevelClient.deleteAsync(deleteRequest, RequestOptions.DEFAULT, actionListener);
        });
    }

    @Override
    public Mono<Void> updateBond(Bond bond) {

/*
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.timeField("updated", new Date());
            builder.field("reason", "daily update");
        }
  */

        //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-update-by-query.html
        UpdateRequest updateRequest=new UpdateRequest("security","bond","1");

        return Mono.<Void>create(sink -> {
            ActionListener<UpdateResponse> actionListener = new ActionListener<UpdateResponse>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    System.out.println("updated successfully!");
                }
                @Override
                public void onFailure(Exception e) {
                    System.out.println("---------failed at update--------");
                }
            };
            restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, actionListener);

        });

    }


    @Override
    public Flux<Bond> findAllUsingProtobuf() {
        System.out.println("finding-----");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5000);  //show up to 200 bonds
        // searchSourceBuilder.query(QueryBuilders.typeQuery("bond"));
        // searchSourceBuilder.query(QueryBuilders.matchQuery("title","bondfarshad"));
        return getBondFluxProtobuf(searchSourceBuilder);
    }



    private Flux<Bond> getBondFluxProtobuf(SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest("security");
        searchRequest.source(searchSourceBuilder);
         return Flux.<Bond>create(sink -> {
            ActionListener<SearchResponse> actionListener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    for(SearchHit hit : searchResponse.getHits()) {
                        ObjectMapper objectMapper = new ObjectMapper();
                        objectMapper.registerModule(new ProtobufModule());
                        try {
                            System.out.println("------trying getBondFluxProtobuf---------");
                            Bond bond = objectMapper.readValue(hit.getSourceAsString(), Bond.class);
                            sink.next(bond);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    sink.complete();
                }
                @Override
                public void onFailure(Exception e) {
                    System.out.println("---------failed at getBondFlux for protobuf---------");
                }
            };
            restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener);
        });
    }



}
