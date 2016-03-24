package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.exceptions.BodyLoadException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by root on 3/24/16.
 */
public class Loader {

    private static final Log LOG = LogFactory.getLog(Loader.class);

    public String load(String link) {
        try {
            System.out.println("loading link " + link);
            HttpClient client = new HttpClient();
            GetMethod method = new GetMethod(link);
            int result = client.executeMethod(method);
            if(result != HttpStatus.SC_OK) {
                LOG.info("Could not load body for " + link + ". Response code " + result);
                return StringUtils.EMPTY;
            }
            return method.getResponseBodyAsString();
        } catch (IOException e) {
            LOG.warn("Fail during loading body");
            return StringUtils.EMPTY;
        }
    }
}
