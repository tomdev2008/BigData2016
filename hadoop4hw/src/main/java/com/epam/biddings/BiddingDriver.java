package com.epam.biddings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * Created by root on 3/30/16.
 */
public class BiddingDriver extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(BiddingDriver.class);

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new BiddingDriver(), args);
        System.exit(code);
    }

    public int run(String[] args) throws Exception {
        LOG.info("Start!");

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Hadoop HW4");
        job.setJarByClass(BiddingDriver.class);

        job.setNumReduceTasks(Integer.parseInt(args[2]));

        job.setPartitionerClass(BiddingsPartitioner.class);
        job.setGroupingComparatorClass(BiddingsGroupingComparator.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BiddingsMapper.class);
        job.setReducerClass(BiddingsReducer.class);

        boolean jobResult = job.waitForCompletion(true);
        if(jobResult) {
            final Text maxiPinYouId = new Text();
            final LongWritable maxValue = new LongWritable();
            job.getCounters()
                    .getGroup(Constants.MAX_I_PIN_YOU_ID_GROUP)
                    .forEach(counter -> {
                        if(maxValue.get() == 0 || maxValue.get() < counter.getValue()) {
                            maxiPinYouId.set(counter.getName());
                            maxValue.set(counter.getValue());
                        }
                    });
            LOG.info("MAX {} {}", maxiPinYouId, maxValue);
            try (
                    FileSystem fileSystem = FileSystem.get(conf);
                    OutputStream outputStream= fileSystem.create(new Path(args[1] + "/max.txt"));
                    PrintWriter printWriter = new PrintWriter(outputStream);
            ) {
                printWriter.println(String.format("%s\t%s", maxiPinYouId, maxValue));
            }
        }

        return jobResult ? 0 : 1;
    }
}
