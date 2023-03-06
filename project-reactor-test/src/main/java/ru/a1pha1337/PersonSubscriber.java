package ru.a1pha1337;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

public class PersonSubscriber implements Subscriber<Person> {

    private static final long DEFAULT_REQ_SIZE = 1;

    private final List<Person> people = new ArrayList<>();

    private Subscription subscription;

    private final long requestSize;

    private final boolean isSlow;

    public PersonSubscriber(boolean isSlow) {
        this(isSlow, DEFAULT_REQ_SIZE);
    }

    public PersonSubscriber(boolean isSlow, long requestSize) {
        this.isSlow = isSlow;
        this.requestSize = requestSize;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(requestSize);
    }

    @Override
    public void onNext(Person person) {
        System.out.println(Thread.currentThread().getName() + " | Received person: " + person);

        if (isSlow) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        people.add(person);

        subscription.request(requestSize);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println(Thread.currentThread().getName() + " | " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("Flow completed!");
    }

    public List<Person> getPeople() {
        return people;
    }
}
