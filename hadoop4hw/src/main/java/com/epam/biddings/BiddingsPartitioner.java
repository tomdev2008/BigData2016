package com.epam.biddings;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 4/1/16.
 */
public class BiddingsPartitioner extends Partitioner<CompositeKey, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(BiddingsPartitioner.class);

    @Override
    public int getPartition(CompositeKey compositeKey, Text value, int numPartitions) {
        return compositeKey.getiPinyouId().hashCode() % numPartitions;
    }
}
