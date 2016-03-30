package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.Splitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Created by Vitaliy on 3/26/2016.
 */
public class SplitterTest {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testNext() throws Exception {
        //given          <--test--->
        String file =   "line 1\nline 2\nline 3\n";
        //               <---10----><---10---->
        assert (file.length() == 21);
        InputStream inputStream = new ByteArrayInputStream(file.getBytes());
        Splitter splitter = new Splitter(inputStream, 0, 10, false);

        //when
        String firstLine = splitter.next();
        String secondLine = splitter.next();
        boolean thirdHasNext = splitter.hasNext();

        //then
        String expectedFirstLine = "line 1";
        String expectedSecondLine = "line 2";
        boolean expectedThirdHasNext = false;
        Assert.assertEquals(expectedFirstLine, firstLine);
        Assert.assertEquals(expectedSecondLine, secondLine);
        Assert.assertEquals(expectedThirdHasNext, thirdHasNext);
    }

    @Test
    public void testNextWhenLineEndsInBlockBoundary() throws Exception {
        //given                     <---test--->
        String file =   "line 1\nline 2\nline3\nline 4\nline 5\n";
        //               <---10----><----10----><----10--->
        InputStream inputStream = new ByteArrayInputStream(file.getBytes());
        Splitter splitter = new Splitter(inputStream, 10, 10, false);

        //when
        String line3 = splitter.next();
        String line4 = splitter.next();
        boolean hasNext5 = splitter.hasNext();

        //then
        Assert.assertEquals("line3", line3);
        Assert.assertEquals("line 4", line4);
        Assert.assertEquals(false, hasNext5);
    }

    @Test
    public void testNextWhenNoNewLineAtEnd() throws Exception {
        //given                     <---test--->
        String file =   "line 1\nline 2\nline3";
        //               <---10----><----10---->
        InputStream inputStream = new ByteArrayInputStream(file.getBytes());
        Splitter splitter = new Splitter(inputStream, 10, 10, false);

        //when
        String line3 = splitter.next();
        boolean hasNext5 = splitter.hasNext();

        //then
        Assert.assertEquals("line3", line3);
        Assert.assertEquals(false, hasNext5);
    }

    @Test
    public void testNextWhenSkipHeader() throws Exception {
        //given          <--test--->
        String file =   "line 1\nline 2\nline3\nline 4\nline 5\n";
        //               <---10----><----10----><----10--->
        InputStream inputStream = new ByteArrayInputStream(file.getBytes());
        Splitter splitter = new Splitter(inputStream, 0, 10, true);

        //when
        String line2 = splitter.next();

        //then
        Assert.assertEquals("line 2", line2);
    }
}