package com.epam.hadoop3hw.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class BiddingsMapReduceTest {

    MapReduceDriver<LongWritable, Text, Text, BiddingsWritable, Text, BiddingsWritable> mapReduceDriver;

    @Before
    public void before() {
        BiddingsMapper mapper = new BiddingsMapper();
        BiddingsReducer reducer = new BiddingsReducer();
        mapReduceDriver =  MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapReduceDriver.withInput(new LongWritable(10), new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "118.254.16.*", "100")));
        mapReduceDriver.withInput(new LongWritable(10), new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "118.254.16.*", "200")));
        mapReduceDriver.withInput(new LongWritable(10), new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "118.254.16.1", "100")));

        //when
        List<Pair<Text, BiddingsWritable>> result = mapReduceDriver.run();

        //then
        Assert.assertThat(result.size(), is(2));

        Text key1 = result.get(0).getFirst();
        Assert.assertThat(key1.toString(), is("118.254.16.*"));

        BiddingsWritable value1 = result.get(0).getSecond();
        Assert.assertThat(value1.getVisits().get(), is(2L));
        Assert.assertThat(value1.getSpends().get(), is(300L));

        Text key2 = result.get(1).getFirst();
        Assert.assertThat(key2.toString(), is("118.254.16.1"));

        BiddingsWritable value2 = result.get(1).getSecond();
        Assert.assertThat(value2.getVisits().get(), is(1L));
        Assert.assertThat(value2.getSpends().get(), is(100L));
    }
}