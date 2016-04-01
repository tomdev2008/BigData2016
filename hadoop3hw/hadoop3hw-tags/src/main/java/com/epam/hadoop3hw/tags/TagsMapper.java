package com.epam.hadoop3hw.tags;

import com.epam.hadoop3hw.tags.parsers.BiddingParser;
import com.epam.hadoop3hw.tags.parsers.TagsParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TagsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(TagsMapper.class);
    public static final String ITEMS_SEPARATOR = "\t";
    public static final int TAGS_POSITION = 1;
    public static final String TAGS_SEPARATOR = " ";

    private Text tag = new Text();
    private LongWritable tagsCount = new LongWritable(1);

    private Map<String, List<String>> tags = new HashMap<>();

    private TagsParser tagsParser = new TagsParser();
    private BiddingParser biddingParser = new BiddingParser();

    protected void setup(Context context) throws IOException, InterruptedException {
        for(URI cacheFile: context.getCacheFiles()) {
            Files.lines(new File(cacheFile.getPath()).toPath())
                    .forEach(line -> {
                        tagsParser.parse(line);
                        if (tagsParser.isSuccess()) {
                            tags.put(tagsParser.getId(), tagsParser.getTags());
                        }
                    });
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        biddingParser.parse(line);
        if(biddingParser.isFailed()) {
            LOG.warn("Could not parse line {}", line);
        }
        String userId = biddingParser.getUserTags();
        for(String tagString: tags.get(userId)) {
            tag.set(tagString);
            context.write(tag, tagsCount);
        }
    }
}