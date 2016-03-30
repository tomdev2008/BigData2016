package com.epam.hadoop3hw.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class BiddingsReducerTest {

    ReduceDriver<Text, BiddingsWritable, Text, BiddingsWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        BiddingsReducer reducer = new BiddingsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduce() throws Exception {
        List<BiddingsWritable> bytesList = Arrays.asList(
                new BiddingsWritable(1L, 10L),
                new BiddingsWritable(1L, 20L),
                new BiddingsWritable(2L, 30L)
        );
        reduceDriver.withInput(new Text("118.254.16.*"), bytesList);

        List<Pair<Text, BiddingsWritable>> result = reduceDriver.run();

        Assert.assertThat(result.size(), is(1));

        Text key = result.get(0).getFirst();
        Assert.assertThat(key.toString(), is("118.254.16.*"));

        BiddingsWritable value = result.get(0).getSecond();
        Assert.assertThat(value.getVisits().get(), is(4L));
        Assert.assertThat(value.getSpends().get(), is(60L));
    }
}