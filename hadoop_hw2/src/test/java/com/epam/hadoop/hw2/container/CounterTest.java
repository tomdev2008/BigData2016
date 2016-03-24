package com.epam.hadoop.hw2.container;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 3/24/16.
 */
public class CounterTest {

    Counter counter;

    @Before
    public void setUp() throws Exception {
        counter = new Counter();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void getTopWords() throws Exception {
        //given
        List<String> words = Arrays.asList("one", "two", "three", "two", "three", "three");
        List<String> expected = Arrays.asList("one", "two");
        //when
        List<String> result = counter.getTopWords(words, 2L);

        //then
        Assert.assertEquals(expected, result);
    }
}