package com.jovisco.tutorials.reactive.common;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class GenericSubscriberImpl implements Subscriber<String> {

    @Getter
    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
    }

    @Override
    public void onNext(String s) {
        log.info("Received: {}", s);
    }



    @Override
    public void onError(Throwable throwable) {
        log.error("Error: {}", throwable.getMessage());
    }

    @Override
    public void onComplete() {
        log.info("Completed!");
    }
}
