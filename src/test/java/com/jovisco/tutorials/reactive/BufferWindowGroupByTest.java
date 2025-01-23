package com.jovisco.tutorials.reactive;

import com.jovisco.tutorials.reactive.common.GenericSubscriberImpl;
import com.jovisco.tutorials.reactive.sec10.FileWriter;
import com.jovisco.tutorials.reactive.sec10.OrderProcessingService;
import com.jovisco.tutorials.reactive.sec10.PurchaseOrder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BufferWindowBatchTest {


    @Test
    @DisplayName("Should handle packages of events from event stream")
    void ShouldBufferEventsFromStreamInAListAfterStreamCompletes() {

        // works only if stream completes - like here with TAKE
        var publisher = eventStream()
                .take(10)
                .buffer();

        StepVerifier.create(publisher)
                .expectNextCount(1)
                .expectComplete()
                .verify();

    }

    @Test
    @DisplayName("Should restrict buffer size")
    void ShouldRestrictBufferSize() {
        // restrict buffer size
        var publisher2 = eventStream()
                .take(10)
                .buffer(5);

        StepVerifier.create(publisher2)
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }

    @Test
    @DisplayName("Should restrict buffer size or waiting time")
    void ShouldRestrictBufferSizeOrTimeout() {
        // restrict buffer size or time
        var publisher3 = eventStream()
                .take(10)
                .bufferTimeout(3, Duration.ofSeconds(1));

        StepVerifier.create(publisher3)
                .expectNext(List.of("event-1", "event-2", "event-3"))
                .expectNextCount(3)
                .expectComplete()
                .verify();

    }

    @Test
    @DisplayName("Should restrict window size and create a new subscriber for each window")
    void ShouldRestrictWindowSize() {
        // restrict window size
        var publisher = eventStream()
                .take(10)
                .windowTimeout(3, Duration.ofSeconds(1))
                .flatMap(this::print)
                .subscribe();

        TestUtility.sleep(3000L);
    }

    private Flux<String> eventStream() {

        return Flux.interval(Duration.ofMillis(200))
                .map(i -> "event-" + (i + 1));
    }

    private Mono<Void> print(Flux<String> flux) {
        return flux.doOnNext(e -> System.out.print("*"))
                .doOnComplete(System.out::println)
                .then();
    }

    @Test
    @DisplayName("Should create a file per window and store events in it")
    void ShouldCreateFilesAndStoreEvents() {

        var counter = new AtomicInteger(0);
        final String fileNameFormat = "src/main/resources/sec10/file%d.txt";

        eventStream()
                .take(10)
                .window(Duration.ofMillis(500))
                .flatMap(flux -> FileWriter.create(
                        flux, Path.of(fileNameFormat.formatted(counter.incrementAndGet()))))
                .subscribe();

        TestUtility.sleep(5000L);
    }

    @Test
    @DisplayName("Flux.groupBy - should create a flux per group")
    void ShouldGroupBy() {

        var publisher = Flux.range(1, 30)
                .delayElements(Duration.ofMillis(300))
                .groupBy(i -> i % 2);

        StepVerifier.create(publisher)
                .expectNextCount(2)
                .expectComplete()
                .verify();

        publisher
                .flatMap(this::processEvent)
                .subscribe(new GenericSubscriberImpl<>("Flux-GroupBy"));

        TestUtility.sleep(10000);
    }

    @Test
    @DisplayName("GroupBy - should group by order type and precess groups differently")
    void ShouldGroupByAndProcessGroupsDifferently() {

        orderStream()
                .filter(OrderProcessingService.canProcess())
                .groupBy(PurchaseOrder::category)
                .flatMap(gf ->
                        gf.transform(OrderProcessingService.getProcessor(gf.key())))
                .subscribe(new GenericSubscriberImpl<>("GroupBy-GroupProcessing"));

        TestUtility.sleep(60000L);

    }

    private static Flux<PurchaseOrder> orderStream() {
        return Flux.interval(Duration.ofMillis(200))
                .map(i -> PurchaseOrder.create());
    }

    private Mono<Void> processEvent(GroupedFlux<Integer, Integer> group) {
        return group
                .doOnNext(i -> log.info("key {}, item {}", group.key(), i))
                .then();
    }
}
