package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.exceptions.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by root on 3/24/16.
 */
public class LinksProcessor {

    private static final Log LOG = LogFactory.getLog(LinksProcessor.class);

    public static final long TOP_N = 10L;

    private Repository repository;
    private Loader loader;
    private Crawler crawler;
    private Counter counter;

    public void process(String srcFilePath, String destinationFilePath) throws IOException { //TODO handle IO
        List<OutputLinkLine> outputLinkLines = repository.readLinks(srcFilePath)
                .parallelStream()
                .map(this::processLine)
                .collect(Collectors.toList());

        repository.write(destinationFilePath, outputLinkLines);
    }

    private OutputLinkLine processLine(InputLinkLine linkLine) {
        try {
            LOG.info("processing line " + linkLine);
            String htmlBody = loader.load(linkLine.getLink());
            List<String> words = crawler.extractWords(htmlBody);
            List<String> topWords = counter.getTopWords(words, TOP_N);
            LOG.info("top words " + topWords);
            return new OutputLinkLine(linkLine, topWords);
        } catch (ParseException e) {
            e.printStackTrace(); //TODO
            throw new RuntimeException(); //TODO
        }
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }

    public void setLoader(Loader loader) {
        this.loader = loader;
    }

    public void setCrawler(Crawler crawler) {
        this.crawler = crawler;
    }

    public void setCounter(Counter counter) {
        this.counter = counter;
    }
}
