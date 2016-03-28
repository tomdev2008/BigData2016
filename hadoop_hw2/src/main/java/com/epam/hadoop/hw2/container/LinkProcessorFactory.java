package com.epam.hadoop.hw2.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.tika.parser.AutoDetectParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by root on 3/24/16.
 */
public class LinkProcessorFactory {

    private FileSystem fileSystem;
    private Crawler crawler;
    private AutoDetectParser parser;
    private Counter counter;
    private Loader loader;

    public LinkProcessorFactory() throws URISyntaxException, IOException {
        parser = new AutoDetectParser();
        crawler = new Crawler();
        counter = new Counter();
        loader = new Loader();
    }

    public LinksProcessor create() throws URISyntaxException, IOException {
        if(fileSystem == null) {
            Configuration conf = new Configuration ();
            URI hdfsUrl = new URI("hdfs:///");
            fileSystem = FileSystem.get(hdfsUrl, conf);
        }
        crawler.setAutoDetectParser(parser);
        LinksProcessor linksProcessor = new LinksProcessor();
        linksProcessor.setFileSystem(fileSystem);
        linksProcessor.setLoader(loader);
        linksProcessor.setCrawler(crawler);
        linksProcessor.setCounter(counter);
        return linksProcessor;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void setCrawler(Crawler crawler) {
        this.crawler = crawler;
    }

    public void setParser(AutoDetectParser parser) {
        this.parser = parser;
    }

    public void setCounter(Counter counter) {
        this.counter = counter;
    }

    public void setLoader(Loader loader) {
        this.loader = loader;
    }
}
