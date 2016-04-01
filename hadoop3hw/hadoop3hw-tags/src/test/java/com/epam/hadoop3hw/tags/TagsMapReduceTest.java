package com.epam.hadoop3hw.tags;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class TagsMapReduceTest {

    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void before() throws URISyntaxException {
        TagsMapper mapper = new TagsMapper();
        TagsReducer reducer = new TagsReducer();
        mapReduceDriver =  MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.addCacheFile(TagsMapReduceTest.class.getResource("local_cache.txt").toURI());
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapReduceDriver.withInput(new LongWritable(10), new Text(String.format(TagsData.BIDDINGS_TEMPLATE, "282163091263")));

        //when
        List<Pair<Text, LongWritable>> result = mapReduceDriver.run();

        //then
        Assert.assertThat(result.size(), is(2));

        Assert.assertThat(result.get(0).getFirst().toString(), is("one"));
        Assert.assertThat(result.get(0).getSecond().get(), is(1L));

        Assert.assertThat(result.get(1).getFirst().toString(), is("two"));
        Assert.assertThat(result.get(1).getSecond().get(), is(2L));
    }
}