package com.epam.biddings;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class BiddingsReducerTest {

    ReduceDriver<CompositeKey, Text, Text, NullWritable> reduceDriver;

    @Before
    public void setUp() throws Exception {
        BiddingsReducer reducer = new BiddingsReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduce() throws Exception {
        CompositeKey key = new CompositeKey();
        key.getiPinyouId().set("AaaId");
        key.getTimestamp().set(10);
        key.getStreamId().set(1);

        List<Text> values = Arrays.asList(new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, 10, "AaaId")));

        reduceDriver.withInput(key, values);

        List<Pair<Text, NullWritable>> result = reduceDriver.run();

        Counter counter = reduceDriver.getCounters().getGroup(Constants.MAX_I_PIN_YOU_ID_GROUP).iterator().next();
        Assert.assertThat(counter.getName(), is("AaaId"));
        Assert.assertThat(counter.getValue(), is(1L));
    }
}