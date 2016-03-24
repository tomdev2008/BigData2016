package com.epam;

import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws TikaException, SAXException, IOException {
        System.out.println( "Hello World!" );


        InputStream html = IOUtils.toInputStream(
                        "<html>" +
                        "<body>" +
                                "qwe ggg" +
                                "<script>(function() {console.log('sdf')})()</script>" +
                                "<br/>" +
                                "<a title=\"sdsdfdfdfdfd\"></a>" +
                                "<span>sss</span>" +
                        "</body>" +
                        "</html>");

        BodyContentHandler bodyContentHandler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        AutoDetectParser parser = new AutoDetectParser();
        parser.parse(html, bodyContentHandler, metadata, new ParseContext());
        System.out.println(bodyContentHandler.toString());


    }
}
