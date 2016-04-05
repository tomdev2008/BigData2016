package com.epam.biddings;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by root on 4/2/16.
 */
public class SortComparator extends WritableComparator {

    public SortComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        int i = k1.getiPinyouId().compareTo(k2.getiPinyouId());
        if(i != 0) {
            return i;
        }
        return k1.getTimestamp().compareTo(k2.getTimestamp());
    }
}
