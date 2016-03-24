package com.epam.hadoop.hw2.container;

import com.epam.hadoop.hw2.container.exceptions.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 3/24/16.
 */
public class Crawler {

    private AutoDetectParser autoDetectParser;

    public List<String> extractWords(String htmlBody) throws ParseException {
        String parsedText = parse(htmlBody);
        Matcher matcher = Pattern.compile("\\b[^\\d\\W]+\\b").matcher(parsedText);
        List<String> words = new ArrayList<>();
        while(matcher.find()) {
            words.add(matcher.group().toLowerCase());
        }
        return words;
//        return Arrays.asList(parsedText.split("\n"))
//                .stream()
//                .flatMap(line -> Stream.of(line.split(" ")))
//                .filter(StringUtils::isNotBlank)
//                .collect(Collectors.toList());
    }

    private String parse(String htmlBody) throws ParseException {
        try {
            BodyContentHandler bodyContentHandler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            AutoDetectParser parser = new AutoDetectParser();
            parser.parse(IOUtils.toInputStream(htmlBody), bodyContentHandler, metadata, new ParseContext());
            return bodyContentHandler.toString();
        } catch (IOException | SAXException | TikaException e) {
            throw new ParseException(e);
        }
    }

    public AutoDetectParser getAutoDetectParser() {
        return autoDetectParser;
    }

    public void setAutoDetectParser(AutoDetectParser autoDetectParser) {
        this.autoDetectParser = autoDetectParser;
    }
}
