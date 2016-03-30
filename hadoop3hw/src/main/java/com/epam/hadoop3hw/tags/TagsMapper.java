package com.epam.hadoop3hw.tags;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TagsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(TagsMapper.class);
    public static final String ITEMS_SEPARATOR = "\t";
    public static final int TAGS_POSITION = 1;
    public static final String TAGS_SEPARATOR = " ";

    private Text tag = new Text();
    private LongWritable tagsCount = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().split(ITEMS_SEPARATOR);
        String tagsString = items[TAGS_POSITION];
        if(StringUtils.isBlank(tagsString)) {
            LOG.info("Tags fields is empty for the line " + value);
            return;
        }
        for(String tagItem: tagsString.split(TAGS_SEPARATOR)) {
            tag.set(tagItem);
            context.write(tag, tagsCount);
        }
    }
}