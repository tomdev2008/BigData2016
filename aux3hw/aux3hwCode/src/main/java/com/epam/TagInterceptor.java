package com.epam;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Vitaliy on 4/24/2016.
 */
public class TagInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(TagInterceptor.class);

    private Map<String, String> tags;

    private String tagsPath;

    private LogParser logParser;

    public TagInterceptor(String tagsPath) {
        this.tagsPath = tagsPath;
        tags = new HashMap<>();
        logParser = new LogParser();
    }

    public void initialize() {
        TagsParser tagsParser = new TagsParser();
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(openStream()))
        ) {
            String line = null;
            while((line = reader.readLine()) != null) {
                tagsParser.parse(line);
                if (tagsParser.isFailed()) {
                    LOG.warn("Could not read tags in line" + line);
                }
                tags.put(tagsParser.getId(), tagsParser.getTags());
            }
            LOG.info("Reading tags completed. Tags count " + tags.size());
        } catch (Exception e) {
            throw new RuntimeException("Could not create tags map.", e);
        }
    }

    private InputStream openStream() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        URI hdfsUrl = new URI("hdfs:///");
        FileSystem fileSystem = FileSystem.get(hdfsUrl, conf);
        return fileSystem.open(new Path(tagsPath));
    }

    public Event intercept(Event event) {
        String line = new String(event.getBody());
        logParser.parse(line);
        if(logParser.isFailed()) {
            LOG.warn("Could not parse log line" + line);
        }
        String userTagsId = logParser.getUserTagsId();
        String tags = this.tags.get(userTagsId);
        if(tags != null) {
            event.getHeaders().put("has_tags", "true");
            String lineWithTags = line + "\t" + tags;
            event.setBody(lineWithTags.getBytes());
        } else {
            event.getHeaders().put("has_tags", "false");
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        List<Event> interceptedEvents = new ArrayList<>();
        for(Event event: list) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }
        return interceptedEvents;
    }

    public void close() {

    }

    public static class Builder
            implements Interceptor.Builder {

        private String tagsPath;

        public void configure(Context context) {
            tagsPath = context.getString("tagsPath");
        }

        public Interceptor build() {
            return new TagInterceptor(tagsPath);
        }
    }
}
