package com.epam;


import com.epam.hadoop.hw2.container.Splitter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Vitaliy on 3/24/2016.
 */
public class App2 {
    public static void main(String[] args) throws IOException {
        File file = new File("D:\\user.profile.tags.us.txt");
        if(!file.exists()) {
            System.out.println("file not found");
            file.createNewFile();
            System.exit(1);
        }
        try (
                InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
                PrintWriter printWriter = new PrintWriter(new File("D://out.txt"))
        ) {

            Splitter splitter = new Splitter(inputStream, 0, 99999999999999999L, true);

            Stream<String> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    splitter, Spliterator.ORDERED | Spliterator.NONNULL), false);

            stream.map(String::toUpperCase)
                    .peek(System.out::println)
                    .forEach(s -> {
                        printWriter.println(s);
                        printWriter.flush();
                    });

            System.out.println("end");
        }














//        Stream.generate(() -> null)
//                .forEach(s3 -> System.out.println(s3));



//        Arrays.asList("qwe", "asd", "zxc")
//                .stream()
//                .map(s -> {
//                    System.out.println(s + "1");
//                    return s;
//                })
//                .map(s1 -> {
//                    System.out.println(s1 + "2");
//                    return s1;
//                })
//                .forEach(s2 -> System.out.println(s2));
    }
}
