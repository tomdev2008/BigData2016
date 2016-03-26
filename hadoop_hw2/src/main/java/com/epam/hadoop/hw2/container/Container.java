package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.CliUtils;
import com.epam.hadoop.hw2.constants.CliConstants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

    public static void main(String[] args) throws TikaException, SAXException, IOException, URISyntaxException, ParseException {


        Options opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - LinksProcessor");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        String offset = cliParser.getOptionValue(CliConstants.OFFSET);
        String length = cliParser.getOptionValue(CliConstants.LENGTH);
        String input = cliParser.getOptionValue(CliConstants.INPUT);
        String output = cliParser.getOptionValue(CliConstants.OUTPUT);

        LOG.info("Started container with" +
                " offset=" + offset +
                " length=" + length +
                " input=" + input +
                " output=" + output);

        LinksProcessor linksProcessor = new LinkProcessorFactory().create();
//        linksProcessor.process(input, output);

        LOG.info("Container completed");
    }
}
