package com.epam.hadoop3hw.tags;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class TagsMapperTest {

    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void before() {
        TagsMapper mapper = new TagsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(10), new Text(String.format(TagsData.TEMPLATE, "one two two")));

        //when
        List<Pair<Text, LongWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(3, result.size());

        Assert.assertThat(result.get(0).getFirst().toString(), is("one"));
        Assert.assertThat(result.get(0).getSecond().get(), is(1L));

        Assert.assertThat(result.get(1).getFirst().toString(), is("two"));
        Assert.assertThat(result.get(1).getSecond().get(), is(1L));

        Assert.assertThat(result.get(2).getFirst().toString(), is("two"));
        Assert.assertThat(result.get(2).getSecond().get(), is(1L));
    }

}