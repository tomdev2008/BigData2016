package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by root on 3/30/16.
 */
public class BiddingsMapper extends Mapper<LongWritable, Text, CompositeKey, > {

    private static final Logger LOG = LoggerFactory.getLogger(BiddingsMapper.class);

    private Parser parser = new Parser();

    private Text ip = new Text();
    private BiddingsWritable biddings = new BiddingsWritable();

    public BiddingsMapper() {
        biddings.getVisits().set(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if(parser.isFailed()) {
            LOG.warn("Could not parse line.");
            return;
        }

        ip.set(parser.getIp());
        biddings.getSpends().set(parser.getBidings());
        context.write(ip, biddings);
    }
}
