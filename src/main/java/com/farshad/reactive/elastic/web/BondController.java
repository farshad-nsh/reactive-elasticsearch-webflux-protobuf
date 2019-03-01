package com.farshad.reactive.elastic.web;

import com.farshad.reactive.elastic.domain.ProcessBond;
import com.farshad.reactive.elastic.securities.bond.Bond;
import com.farshad.reactive.elastic.securities.bond.BondRepository;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.elasticsearch.search.SearchHit;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;


/**
 * @author farshad
 * @version  1.0.0
 */

@RestController
@RequestMapping("/bonds")
public class BondController {

    private final BondRepository bondRepository;

    public BondController(BondRepository bondRepository) {
        this.bondRepository = bondRepository;
    }

    @GetMapping("/getAllBonds")
    public Flux<Bond> findAll(){
        return bondRepository.findAll()
                .onErrorResume(error -> Flux.empty());
    }


    @GetMapping("/getAllBonds2")
    public List<Bond> findAll2() throws IOException {
        return bondRepository.findAll2();
    }

    @GetMapping(path="/getAllBondsUsingProtobuf")
    public Flux<Bond> findAllUsingProtobuf(){
        return bondRepository.findAllUsingProtobuf();
    }


/*
    @RequestMapping(value = "protoBuf", produces = "application/x-protobuf")
    public PersonProto getPersonProto() {
        return  PersonProto
                .newBuilder()
                .setFirstName("Jake")
                .setLastName("Partusch")
                .setEmailAddress("jakepartusch@abc.com")
                .setHomeAddress("123 Seasame Street")
                .addPhoneNumbers(PersonProto.PhoneNumber
                        .newBuilder()
                        .setAreaCode(123)
                        .setPhoneNumber(1234567))
                .build();
    }
  */


    @PostMapping(path="/addBond",consumes = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Void> add(@RequestBody Bond bond) throws IOException {
        return bondRepository.addDocument(bond);
    }

/*
without using flux and mono
 */
    @PostMapping(path="/addBond2",consumes = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseStatus(HttpStatus.CREATED)
    public Bond add2(@RequestBody Bond bond) throws IOException {
        return bondRepository.addDocument2(bond);
    }


    @GetMapping("/term")
    @ResponseStatus(HttpStatus.OK)
    public Flux<Bond> findByTitle(@RequestParam(name="title") String title){
        return bondRepository.searchTermQueryByTitle(title);
    }

    @GetMapping("/match")
    @ResponseStatus(HttpStatus.OK)
    public Flux<Bond> matchByTitle(@RequestParam(name="title") String title){

        return bondRepository.searchMatchPhraseQueryByTitle(title);
    }


    @GetMapping("/deleteIndex")
    @ResponseStatus(HttpStatus.OK)
    public Flux<Bond> deleteIndex(@RequestParam(name="index") String index){
        return bondRepository.deleteByIndex(index);
    }


    //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-update.html
    @PostMapping(path="/updateBond",consumes = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Void> updateBond(@RequestBody Bond bond){
        return bondRepository.updateBond(bond);
    }


    @GetMapping("/process")
    @ResponseStatus(HttpStatus.OK)
    public Flux<Bond> process(@RequestParam(name="index") String index){
        ProcessBond processBond=new ProcessBond();
        return processBond.doAccountingAtMofid();
    }


}
