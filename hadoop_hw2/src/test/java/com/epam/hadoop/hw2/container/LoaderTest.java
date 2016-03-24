package com.epam.hadoop.hw2.container;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 3/24/16.
 */
public class LoaderTest {

    private Loader loader;

    @Before
    public void setUp() throws Exception {
        loader = new Loader();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void load() throws Exception {
        //given
        String link = "http://www.miniinthebox.com/obdmate-diagnostic-tool-obd2-obdii-eobd-code-reader-om510-gasoline-cars-and-a-part-of-diesel-cars_p1516838.html";

        //when
        String htmlBody = loader.load(link);

        //then
        System.out.println(htmlBody);
    }
}