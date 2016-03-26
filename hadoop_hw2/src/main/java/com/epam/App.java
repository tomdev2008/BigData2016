package com.epam;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws TikaException, SAXException, IOException, URISyntaxException {
        Configuration conf = new Configuration ();
        URI hdfsUrl = new URI("hdfs://sandbox/");
        FileSystem fileSystem = FileSystem.get(hdfsUrl, conf);

        Path path = new Path("/tmp/hadoop2hw/user.profile.tags.us.test3.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0L, fileStatus.getLen());

        //fileSystem.getFileBlockLocations(fileStatus, 402653184L, 10L)



        try (FSDataInputStream fsDataInputStream = fileSystem.open(path)) {
            byte[] bytes = new byte[1000];
            int skiped = fsDataInputStream.skipBytes(402653184);
            int read = fsDataInputStream.read(bytes);
            System.out.println(read);
            System.out.println(bytes);
            System.out.println(new String(bytes));
        }

        System.out.println("exit");
    }
}
