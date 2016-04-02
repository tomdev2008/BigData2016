package com.epam.biddings;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by root on 4/2/16.
 */
public class BiddingsGroupingComparator extends WritableComparator {

    public BiddingsGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        return k1.getiPinyouId().compareTo(k2.getiPinyouId());
    }
}
