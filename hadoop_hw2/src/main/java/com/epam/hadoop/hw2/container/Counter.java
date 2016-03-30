package com.epam.hadoop.hw2.container;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by root on 3/24/16.
 */
public class Counter {

    public List<String> getTopWords(List<String> words, Long count) {
        Map<String, Long> countedWords = words
                .stream()
                .collect(Collectors.groupingBy(String::toLowerCase, Collectors.summingLong(value -> 1)));
        return countedWords
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .limit(count)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

}
