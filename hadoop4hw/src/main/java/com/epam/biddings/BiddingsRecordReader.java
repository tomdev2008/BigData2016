package com.epam.biddings;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by root on 4/3/16.
 */
public class BiddingsRecordReader extends RecordReader<Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(BiddingsRecordReader.class);

    private Parser parser;

    private final LineRecordReader lineRecordReader;

    private Text innerValue;

    private Text key;

    private Text value;

    public Class getKeyClass() { return Text.class; }

    public BiddingsRecordReader(Configuration conf)
            throws IOException {

        lineRecordReader = new LineRecordReader();
        parser = new Parser();
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        lineRecordReader.initialize(genericSplit, context);
    }

    public synchronized boolean nextKeyValue()
            throws IOException {
        byte[] line = null;
        int lineLen = -1;
        if (lineRecordReader.nextKeyValue()) {
            innerValue = lineRecordReader.getCurrentValue();
            line = innerValue.getBytes();
            lineLen = innerValue.getLength();
        } else {
            return false;
        }
        if (StringUtils.isBlank(line.toString()))
            return false;
        if (key == null) {
            key = new Text();
        }
        if (value == null) {
            value = new Text();
        }
        parser.parse(innerValue.toString());
        if(parser.isFailed()) {
            key = new Text();
            value = new Text();
            LOG.warn("Could not parse line {}" + innerValue);
            return false;
        }
        key.set(parser.getiPinyouId());
        value.set(innerValue);
        return true;
    }

    public Text getCurrentKey() {
        return key;
    }

    public Text getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    public synchronized void close() throws IOException {
        lineRecordReader.close();
    }
}
