package ru.a1pha1337;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.awaitility.Awaitility.await;

public class TestClass {

    private static final long PEOPLE_TO_GENERATE = 500L;

    private List<Person> people;

    private String[] names = {
            "Maxim", "Zhenya", "Petr", "Artyom", "Tanya",
            "Sasha", "Anya", "Sergey", "Anton", "Dima"
    };

    @Before
    public void init() {
        Random random = new Random();
        AtomicLong id = new AtomicLong(0);

        people = Stream.generate(() -> Person.builder()
                .id(id.getAndIncrement())
                .name(names[random.nextInt(names.length)])
                .age(ThreadLocalRandom.current().nextInt(10, 80))
                .build()
        ).limit(PEOPLE_TO_GENERATE).collect(Collectors.toList());
    }

    @Test
    public void givenDefaultPublisher_whenConsumerIsEnoughFast() {
        PersonSubscriber subscriber = new PersonSubscriber(false);

        try (SubmissionPublisher<Person> publisher = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 1)) {
            publisher.subscribe(subscriber);

            for (Person person : people) {
                System.out.println(Thread.currentThread().getName() + " | Publishing person: " + person);
                publisher.submit(person);
            }
        }

        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(
                () -> assertEquals(people, subscriber.getPeople())
        );
    }

    @Test
    public void givenBufferingPublisher_whenConsumerIsTooSlow() {
        PersonSubscriber subscriber = new PersonSubscriber(true);

        try (SubmissionPublisher<Person> publisher = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 512)) {
            publisher.subscribe(subscriber);

            for (Person person : people) {
                System.out.println(Thread.currentThread().getName() + " | Publishing person: " + person);
                publisher.submit(person);
            }
        }

        await().untilAsserted(
                () -> assertEquals(people, subscriber.getPeople())
        );
    }

    @Test
    public void givenDroppingPublisher_whenConsumerIsTooSlow() {
        PersonSubscriber subscriber = new PersonSubscriber(true);

        try (SubmissionPublisher<Person> publisher = new SubmissionPublisher<>()) {
            publisher.subscribe(subscriber);

            for (Person person : people) {
                System.out.println(Thread.currentThread().getName() + " | Publishing person: " + person);
                publisher.offer(person, (s, a) -> {
                    s.onError(new Exception("Can't handle backpressure. Dropping value: " + person));
                    return true;
                });
            }
        }

        await().untilAsserted(
                () -> assertNotEquals(people, subscriber.getPeople())
        );
    }
}
