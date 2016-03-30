package com.epam.hadoop.hw2.container;

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
        String containerId = System.getenv("CONTAINER_ID");

        LOG.info("Initialization container " + containerId);
        System.out.println("Initialization container " + containerId);

        Options opts = new Options();
        opts.addOption(CliConstants.OFFSET, true, "Offset");
        opts.addOption(CliConstants.LENGTH, true, "Length");
        opts.addOption(CliConstants.INPUT, true, "Input");
        opts.addOption(CliConstants.OUTPUT, true, "Output");

        CommandLine cliParser = new GnuParser().parse(opts, args);

        String input = cliParser.getOptionValue(CliConstants.INPUT);
        String output = cliParser.getOptionValue(CliConstants.OUTPUT);
        Long offset = Long.parseLong(cliParser.getOptionValue(CliConstants.OFFSET));
        Long length = Long.parseLong(cliParser.getOptionValue(CliConstants.LENGTH));

        LOG.info("Started container " + containerId + " with" +
                " input=" + input +
                " output=" + output +
                " offset=" + offset +
                " length=" + length);

        LinksProcessor linksProcessor = new LinkProcessorFactory().create();
        linksProcessor.process(input, output, offset, length, containerId);

        LOG.info("Container completed");
    }
}
