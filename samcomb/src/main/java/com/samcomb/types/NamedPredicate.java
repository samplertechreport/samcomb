package com.samcomb.types;

import java.util.HashMap;
import java.util.function.Predicate;

public class NamedPredicate<T> implements Predicate<T> {
    private final String name;
    private final Predicate<T> predicate;

    public NamedPredicate(String name, Predicate<T> predicate) {
        this.name = name;
        this.predicate = predicate;
    }

    @Override
    public boolean test(T t) {
        return predicate.test(t);
    }

    @Override
    public String toString() {
        return name;
    }

    public static void main(String... args) {
        HashMap<String, NamedPredicate<Object>> predicate = new HashMap<>();
        NamedPredicate<Object> isEven = new NamedPredicate<>("isEven", i -> (int) i % 2 == 0);
        NamedPredicate<Object> equalToC = new NamedPredicate<>("isEven", i -> i == "C");
        predicate.put("isEven", isEven);
        predicate.put("equalToC", equalToC);
        boolean b1 = predicate.get("isEven").test(2);
        String c = "B";
        boolean b2 = predicate.get("equalToC").test(c);
        System.out.println(b1); // prints isEven
        System.out.println(b2); //
    }
}
