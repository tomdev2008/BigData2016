package com.epam.hadoop.hw2.container.exceptions;

/**
 * Created by Vitaliy on 3/24/2016.
 */
public class BodyLoadException extends Exception {
    public BodyLoadException(String message) {
        super(message);
    }
    public BodyLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
