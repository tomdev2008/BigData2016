package com.epam.hadoop3hw.tags;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class TagsReducerTest {

    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        TagsReducer reducer = new TagsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduce() throws Exception {
        reduceDriver.withInput(new Text("one"), Collections.singletonList(new LongWritable(1)));
        reduceDriver.withInput(new Text("two"), Arrays.asList(new LongWritable(1), new LongWritable(1)));

        List<Pair<Text, LongWritable>> result = reduceDriver.run();

        Assert.assertThat(result.size(), is(2));

        Assert.assertThat(result.get(0).getFirst().toString(), is("one"));
        Assert.assertThat(result.get(0).getSecond().get(), is(1L));

        Assert.assertThat(result.get(1).getFirst().toString(), is("two"));
        Assert.assertThat(result.get(1).getSecond().get(), is(2L));

    }
}