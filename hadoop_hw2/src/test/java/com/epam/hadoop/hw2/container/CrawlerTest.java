package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.exceptions.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by root on 3/24/16.
 */
public class CrawlerTest {

    private Crawler crawler;

    @Before
    public void setUp() throws Exception {
        AutoDetectParser parser = new AutoDetectParser();

        crawler = new Crawler();
        crawler.setAutoDetectParser(parser);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void test() throws ParseException, IOException {
        //given
        String inputHtml = IOUtils.toString(CrawlerTest.class.getResourceAsStream("html-body-input.txt"));
        List<String> expectedWords = Arrays.asList("first", "second", "third");

        //when
        List<String> words = crawler.extractWords(inputHtml);

        //then
        Assert.assertEquals(expectedWords, words);
    }
}