package com.farshad.reactive.elastic.domain;

import com.farshad.reactive.elastic.securities.bond.Bond;
import io.reactivex.*;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Subscription;
import org.springframework.scheduling.concurrent.ThreadPoolExecutorFactoryBean;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.*;




public class ProcessBond {

     public Flux<Bond> doAccountingAtMofid(){
        System.out.println("accounting process started!");


        Bond bond1=new Bond();
        Bond bond2=new Bond();


         Observable<Bond> bonds = Observable.just(bond1,bond2);

         bonds.startWith(bond1).subscribe(niceBond -> {
             System.out.println(niceBond.getPrice());
             System.out.println(niceBond.getTitle());
         });

         Observable<Bond> obsbond1 = Observable.just(bond1);
         Observable<Bond> obsbond2 = Observable.just(bond2);


         Observable<String> combinedBonds= obsbond1.zipWith(obsbond2, (one, two) -> one.getPrice() + " " + two.getPrice());

         Observable<Bond> advancedBond=combinedBonds.map(new Function<String, Bond>() {
                              @Override
                              public Bond apply(String s) throws Exception {
                                  Bond b1=new Bond();
                                  b1.setTitle(s);
                                return b1;
                              }
                          });

         Observer<Bond> observer=new Observer<Bond>() {
             @Override
             public void onSubscribe(Disposable d) {


             }

             @Override
             public void onNext(Bond value) {
                 System.out.println("The name of bond is:"+value.getTitle());
             }

             @Override
             public void onError(Throwable e) {
                 System.out.println(e.getCause());
             }

             @Override
             public void onComplete() {
                 //getting price of bond
                 advancedBond.subscribe(bond -> {
                     bond.getPrice();

                 });
             }
         };

         ThreadFactory threadFactory=new ThreadPoolExecutorFactoryBean();
         Executor executor=new ThreadPoolExecutor(10,
                 10,0L, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<>(1000),threadFactory
                 );

         Scheduler scheduler1=Schedulers.from(executor);

         Observable<Bond> o1= Observable.just(new Bond());
         o1.subscribeOn(scheduler1).subscribe(b->{
             System.out.println(b.getPrice());
         });

         Observable<Bond> allbonds=Observable.create(new ObservableOnSubscribe<Bond>() {
             @Override
             public void subscribe(ObservableEmitter<Bond> e) throws Exception {
                 e.onNext(new Bond());
                 e.onNext(new Bond());
                 System.out.println("finished getting bonds!");
             }
         });

        Flux <Bond> processedBond = Flux.from(advancedBond.toFlowable(BackpressureStrategy.BUFFER));

        return processedBond;

    }


}
