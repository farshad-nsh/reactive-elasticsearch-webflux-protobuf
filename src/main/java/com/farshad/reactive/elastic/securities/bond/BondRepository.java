package com.farshad.reactive.elastic.securities.bond;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;

@Service
public interface BondRepository {

    Mono<Void> addDocument(Bond bond) throws IOException;

    Bond addDocument2(Bond bond) throws IOException;


    Flux<Bond> searchTermQueryByTitle(String title);

    Flux<Bond> searchMatchPhraseQueryByTitle(String title);

    Flux<Bond> findAll();

    List<Bond> findAll2() throws IOException;

    Mono<Bond> findById(String id);


    public Flux<Bond> deleteByIndex(String index);


    Mono<Void> updateBond(Bond bond);

    Flux<Bond> findAllUsingProtobuf();
}

