package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.exceptions.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by root on 3/24/16.
 */
public class LinksProcessor {

    private static final Log LOG = LogFactory.getLog(LinksProcessor.class);

    public static final long TOP_N = 10L;

    public static final String ITEMS_SEPARATOR = "\t";
    public static final int WORDS_POSITION = 1;

    private Repository repository;
    private Loader loader;
    private Crawler crawler;
    private Counter counter;

    private FileSystem fileSystem;

    public void process(String srcFilePath, String destinationFilePath, Long offset, Long length) throws IOException { //TODO handle IO
        try (
                FSDataInputStream inputStream = fileSystem.open(new Path(srcFilePath));
                BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
                FSDataOutputStream outputStream = fileSystem.create(new Path(destinationFilePath));
                PrintWriter writer = new PrintWriter(outputStream)
        ) {
            Splitter splitter = new Splitter(bufferedInputStream, offset, length, true);

            Stream<String> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    splitter, Spliterator.ORDERED | Spliterator.NONNULL), false);
            stream.map(this::mapToInputLinkLine)
                    .map(this::processLine)
                    .forEach(outputLinkLine -> write(writer, outputLinkLine));
        }
    }

    private InputLinkLine mapToInputLinkLine(String line) {
        ArrayList<String> lineItems = new ArrayList<>(Arrays.asList(line.split(ITEMS_SEPARATOR)));
        return new InputLinkLine(lineItems);
    }

    private void write(PrintWriter writer, OutputLinkLine outputLinkLine) {
        InputLinkLine inputLinkLine = outputLinkLine.getInputLinkLine();
        ArrayList<String> lineItems = inputLinkLine.getLineItems();
        String concatenatedWords = outputLinkLine
                .getWords()
                .stream()
                .collect(Collectors.joining(" "));
        lineItems.remove(WORDS_POSITION);
        lineItems.add(WORDS_POSITION, concatenatedWords);
        String line = lineItems
                .stream()
                .collect(Collectors.joining("\t"));
        writer.println(line);
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

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
}
