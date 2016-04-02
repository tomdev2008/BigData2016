package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class BiddingsMapperTest {

    MapDriver<LongWritable, Text, CompositeKey, Text> mapDriver;

    @Before
    public void before() {
        BiddingsMapper mapper = new BiddingsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws Exception {
        //given
        Text line = new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "10", "AaaId"));
        mapDriver.withInput(new LongWritable(10), line);


        //when
        List<Pair<CompositeKey, Text>> result = mapDriver.run();

        //then
        Assert.assertEquals(1, result.size());

        CompositeKey outKey = result.get(0).getFirst();
        Assert.assertThat(outKey.getiPinyouId().toString(), is("AaaId"));
        Assert.assertThat(outKey.getTimestamp().get(), is(10L));

        Text outValue = result.get(0).getSecond();
        Assert.assertThat(outValue, is(line));
    }

}