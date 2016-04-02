package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Vitaliy on 3/31/2016.
 */
public class CompositeKey implements WritableComparable {

    private Text iPinyouId = new Text();
    private LongWritable timestamp = new LongWritable();
    private LongWritable streamId = new LongWritable();

    public int compareTo(Object o) {
        CompositeKey other = (CompositeKey) o;
        int i = iPinyouId.compareTo(other.getiPinyouId());
        if(i != 0) {
            return i;
        }
        return timestamp.compareTo(other.getTimestamp());
    }

    public void write(DataOutput out) throws IOException {
        iPinyouId.write(out);
        timestamp.write(out);
        streamId.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        iPinyouId.readFields(in);
        timestamp.readFields(in);
        streamId.readFields(in);
    }

    public Text getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(Text iPinyouId) {
        this.iPinyouId = iPinyouId;
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LongWritable timestamp) {
        this.timestamp = timestamp;
    }

    public LongWritable getStreamId() {
        return streamId;
    }

    public void setStreamId(LongWritable streamId) {
        this.streamId = streamId;
    }
}
