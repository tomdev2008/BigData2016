package com.epam.hadoop.hw2.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by root on 3/23/16.
 */
public class Container {

    private static final Log LOG = LogFactory.getLog(Container.class);

    public static void main(String[] args) throws TikaException, SAXException, IOException, URISyntaxException {
        LOG.info("Started container");

        LinksProcessor linksProcessor = new LinkProcessorFactory().create();
        linksProcessor.process(args[0], args[1]);

        LOG.info("Container completed");
    }
}
