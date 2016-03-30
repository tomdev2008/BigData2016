package biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by root on 3/30/16.
 */
public class BiddingsMapper extends Mapper<LongWritable, Text, Text, BiddingsWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);
    public static final int LINE_ITEMS_COUNT = 22;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineItems = value.toString().split("\t");
        if(lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wring numbers of items in line " + value);
        }


    }
}
