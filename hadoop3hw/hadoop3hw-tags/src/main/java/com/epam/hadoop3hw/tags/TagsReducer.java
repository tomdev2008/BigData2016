package com.epam.hadoop3hw.tags;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class TagsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable totalSum = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for(LongWritable tagsCount: values) {
            sum += tagsCount.get();
        }
        totalSum.set(sum);
        context.write(key, totalSum);
    }
}
