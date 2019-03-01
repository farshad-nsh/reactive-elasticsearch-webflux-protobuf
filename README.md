# Reactive ElasticSearch by Webflux and Netty(spring boot2)  
First there is a request to see all bonds(a financial security)

<code>

    @GetMapping(path="/getAllBondsUsingProtobuf")
      public Flux<Bond> findAllUsingProtobuf(){
          return bondRepository.findAllUsingProtobuf();
    }
</code> 

Then we return a flux of bond by converting protobuf to json. Note that protocol buffer over the
wire is not used and i only use for conversion of protobuf to json since
in most applications we want the backward and forward compatibility
and maintaining and integrity of  the structure of contexts in domain driven design.

<code>

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
</code>

###  compile
 protoc Bond.proto  --java_out=./
 
 
 
 
 