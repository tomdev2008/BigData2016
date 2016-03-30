package com.epam.hadoop3hw.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers.*;

import java.util.List;

import static org.hamcrest.CoreMatchers.*;

public class BiddingsMapperTest {

    MapDriver<LongWritable, Text, Text, BiddingsWritable> mapDriver;

    @Before
    public void before() {
        BiddingsMapper mapper = new BiddingsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(10), new Text(String.format(BiddingsData.BIDDINGS_TEMPLATE, "118.254.16.*", "100")));


        //when
        List<Pair<Text, BiddingsWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(1, result.size());

        Text outKey = result.get(0).getFirst();
        Assert.assertThat(outKey.toString(), is("118.254.16.*"));

        BiddingsWritable outValue = result.get(0).getSecond();
        Assert.assertThat(1L, is(outValue.getVisits().get()));
        Assert.assertThat(100L, is(outValue.getSpends().get()));
    }

}