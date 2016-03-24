package com.epam.hadoop.hw2.container;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 3/24/16.
 */
public class RepositoryTest {

    public static final String INPUT_FILE_NAME = "input.txt";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    Repository repository;

    FileSystem fileSystem;

    private String inputFilePath;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.getLocal(configuration);

        repository = new Repository();
        repository.setFileSystem(fileSystem);

        inputFilePath = RepositoryTest.class.getResource(INPUT_FILE_NAME).getPath();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void readLinks() throws Exception {
        //given
        //when
        List<InputLinkLine> links = repository.readLinks(inputFilePath);

        //then
        Assert.assertEquals(1, links.size());
        Assert.assertEquals("http://testhost.com/test.html", links.get(0).getLink());
    }

    @Test
    public void write() throws Exception {
        //given
        String filePath = temporaryFolder.getRoot().toString() + File.pathSeparator + "/result.txt";
        List<OutputLinkLine> outputLinkLines = new ArrayList<>();
        InputLinkLine line = new InputLinkLine(new ArrayList(Arrays.asList("282163091263", "ON", "CPC","BROAD","http://testhost.com/test.html")));
        List<String> words = Arrays.asList("one", "two");
        outputLinkLines.add(new OutputLinkLine(line, words));

        //when
        repository.write(filePath, outputLinkLines);

        //then
        String expectedResult = "282163091263\tone two\tON\tCPC\tBROAD\thttp://testhost.com/test.html\n";
        String result = FileUtils.readFileToString(new File(filePath));
        Assert.assertEquals(expectedResult, result);
    }
}