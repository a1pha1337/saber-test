package ru.a1pha1337;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestClass {

    private final Random random = new Random();

    private final String[] names = {
            "Maxim", "Zhenya", "Petr", "Artyom", "Tanya",
            "Sasha", "Anya", "Sergey", "Anton", "Dima"
    };

    /**
     * Requesting necessary number of items
     */
    @Test
    public void whenRequestChunk_thenProcessMessages() {
        List<Person> people = generatePeople(10);
        Flux<Person> publisher = Flux.fromStream(people.stream());

        long requestSize = 2L;
        publisher.log().subscribe(new BaseSubscriber<>() {
            long requestNumber = 0;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestSize);
            }

            @Override
            protected void hookOnNext(Person value) {
                requestNumber++;
                System.out.println(Thread.currentThread().getName() + " | " + value);

                if (requestNumber % requestSize == 0) {
                    request(requestSize);
                }
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("Flow Completed");
            }
        });
    }

    /**
     * Subscription cancellation
     */
    @Test
    public void whenCancelInvoked_thenUnsubscribe() {
        Flux<Person> publisher = Flux.fromStream(generatePeople(10).stream());

        publisher.log().subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(5);
            }

            @Override
            protected void hookOnNext(Person value) {
                System.out.println(Thread.currentThread().getName() + " | " + value);

                // We can cancel the subscription at any time
                cancel();
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("Flow Completed");
            }
        });
    }

    private List<Person> generatePeople(long number) {
        AtomicLong id = new AtomicLong(0);

        return Stream.generate(() -> Person.builder()
                .id(id.getAndIncrement())
                .name(names[random.nextInt(names.length)])
                .age(ThreadLocalRandom.current().nextInt(10, 80))
                .build()
        ).limit(number).collect(Collectors.toList());
    }
}
