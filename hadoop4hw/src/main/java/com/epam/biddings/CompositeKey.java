package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Vitaliy on 3/31/2016.
 */
public class CompositeKey implements WritableComparable {

    private LongWritable timestamp;
    private Text iPinyouId;

    public int compareTo(Object o) {
        CompositeKey other = (CompositeKey) o;
        int i = iPinyouId.compareTo(other.getiPinyouId());
        if(i != 0) {
            return i;
        }
        return timestamp.compareTo(other.getTimestamp());
    }

    public void write(DataOutput out) throws IOException {
        timestamp.write(out);
        iPinyouId.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        timestamp.readFields(in);
        iPinyouId.readFields(in);
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LongWritable timestamp) {
        this.timestamp = timestamp;
    }

    public Text getiPinyouId() {
        return iPinyouId;
    }

    public void setiPinyouId(Text iPinyouId) {
        this.iPinyouId = iPinyouId;
    }
}
