package com.epam.hadoop.hw2.utils;

/**
 * Created by Vitaliy on 3/26/2016.
 */
public class CliUtils {
    public static String param(String name, long value) {
        return "--" + name + " " + value;
    }
    public static String param(String name, String value) {
        return "--" + name + " " + value;
    }
}
