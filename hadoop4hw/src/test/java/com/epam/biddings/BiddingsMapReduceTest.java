package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class BiddingsMapReduceTest {

    MapReduceDriver<LongWritable, Text, CompositeKey, Text, Text, NullWritable> mapReduceDriver;

    @Before
    public void before() {
        BiddingsMapper mapper = new BiddingsMapper();
        BiddingsReducer reducer = new BiddingsReducer();
        mapReduceDriver =  MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMap() throws Exception {
        //given
        Text line = new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "10", "AaaId"));
        mapReduceDriver.withInput(new LongWritable(10), line);


        //when
        List<Pair<Text, NullWritable>> result = mapReduceDriver.run();


        //then
        Counter counter = mapReduceDriver.getCounters().getGroup(Constants.MAX_I_PIN_YOU_ID_GROUP).iterator().next();
        Assert.assertThat(counter.getName(), is("AaaId"));
        Assert.assertThat(counter.getValue(), is(1L));

    }
}