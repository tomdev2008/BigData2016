package com.epam.hadoop.hw2.container;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;

/**
 * Created by root on 3/24/16.
 */
public class Loader {

    public String load(String link) {
        try {
            HttpClient client = new HttpClient();
            GetMethod method = new GetMethod(link);
            int result = client.executeMethod(method);
            if(result != HttpStatus.SC_OK) {
                throw new RuntimeException(); //TODO
            }
            return method.getResponseBodyAsString();
        } catch (IOException e) {
            throw new RuntimeException(); //TODO
        }
    }
}
