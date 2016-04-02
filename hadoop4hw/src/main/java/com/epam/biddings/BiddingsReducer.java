package com.epam.biddings;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class BiddingsReducer extends Reducer<CompositeKey, Text, Text, NullWritable> {

    private NullWritable outValue = NullWritable.get();

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter(Constants.MAX_I_PIN_YOU_ID_GROUP, key.getiPinyouId().toString());

        if(key.getStreamId().get() == 1) {
            counter.increment(1);
        }

        for(Text value: values) {
            context.write(value, outValue);
        }
    }
}
