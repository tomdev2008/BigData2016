//package com.epam.biddings;
//
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//
///**
// * Created by Vitaliy on 3/31/2016.
// */
//public class CompositeKeyComparator extends WritableComparator {
//    protected CompositeKeyComparator() {
//        super(CompositeKey.class, true);
//    }
//    @Override
//    public int compare(WritableComparable w1, WritableComparable w2) {
//        CompositeKey k1 = (CompositeKey)w1;
//        CompositeKey k2 = (CompositeKey)w2;
//
//        int result = k1.getSymbol().compareTo(k2.getSymbol());
//        if(0 == result) {
//            result = -1* k1.getTimestamp().compareTo(k2.getTimestamp());
//        }
//        return result;
//    }
//}