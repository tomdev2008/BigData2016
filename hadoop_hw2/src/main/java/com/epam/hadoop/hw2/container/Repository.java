package com.epam.hadoop.hw2.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by root on 3/24/16.
 */
public class Repository {

    private static final Log LOG = LogFactory.getLog(Container.class);
    public static final String ITEMS_SEPARATOR = "\t";
    public static final int WORDS_POSITION = 1;


    private FileSystem fileSystem;

    public List<InputLinkLine> readLinks(String filePath) throws IOException { //TODO handle IO
        List<InputLinkLine> linkLines = new ArrayList<>();
        try (
            FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader)
        ) {
            LOG.info("Reading file " + filePath);
            String header = reader.readLine();
            LOG.debug("Skip header '" + header + "'");
            String line;
            while ((line = reader.readLine()) != null) {
                ArrayList<String> lineItems = new ArrayList<>(Arrays.asList(line.split(ITEMS_SEPARATOR)));
                linkLines.add(new InputLinkLine(lineItems));
            }
            LOG.info("Read " + linkLines.size() + " links");
        }
        return linkLines;
    }

    public void write(String filePath, List<OutputLinkLine> outputLinkLines) throws IOException { //TODO handle IO
        try (
            FSDataOutputStream outputStream = fileSystem.create(new Path(filePath));
            PrintWriter writer = new PrintWriter(outputStream)
        ) {
            for(OutputLinkLine outputLinkLine: outputLinkLines) {
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
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }
}
