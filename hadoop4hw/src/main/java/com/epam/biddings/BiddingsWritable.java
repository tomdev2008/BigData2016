package com.epam.biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BiddingsWritable implements Writable {

    private LongWritable visits = new LongWritable();

    private LongWritable spends = new LongWritable();

    public BiddingsWritable() {
    }

    public BiddingsWritable(LongWritable visits, LongWritable spends) {
        this.visits = visits;
        this.spends = spends;
    }

    public BiddingsWritable(Long visits, Long spends) {
        this.visits = new LongWritable(visits);
        this.spends = new LongWritable(spends);
    }

    public void write(DataOutput dataOutput) throws IOException {
        visits.write(dataOutput);
        spends.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        visits.readFields(dataInput);
        spends.readFields(dataInput);
    }

    public LongWritable getVisits() {
        return visits;
    }

    public void setVisits(LongWritable visits) {
        this.visits = visits;
    }

    public LongWritable getSpends() {
        return spends;
    }

    public void setSpends(LongWritable spends) {
        this.spends = spends;
    }
}
