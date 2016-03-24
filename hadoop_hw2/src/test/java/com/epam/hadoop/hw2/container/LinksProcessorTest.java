package com.epam.hadoop.hw2.container;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by root on 3/24/16.
 */
public class LinksProcessorTest {

    public static final String INPUT_FILE_NAME = "LinksProcessorTest-input.txt";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    LinksProcessor linksProcessor;

    private String inputFilePath;



    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.getLocal(configuration);

        LinkProcessorFactory factory = new LinkProcessorFactory();
        factory.setFileSystem(fileSystem);
        linksProcessor = factory.create();

        inputFilePath = RepositoryTest.class.getResource(INPUT_FILE_NAME).getPath();
    }

    @Test
    public void process() throws Exception {
        //given
        String outputFilePath = temporaryFolder.getRoot().toString() + File.pathSeparator + "/result.txt";

        //when
        linksProcessor.process(inputFilePath, outputFilePath);

        //then
        String expectedResult = "282163091263\tone two\tON\tCPC\tBROAD\thttp://testhost.com/test.html\n";
        String result = FileUtils.readFileToString(new File(outputFilePath));
        Assert.assertEquals(expectedResult, result);
    }
}