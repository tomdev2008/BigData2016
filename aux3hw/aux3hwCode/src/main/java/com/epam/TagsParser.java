package com.epam;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class TagsParser {

    private static final Logger LOG = LoggerFactory.getLogger(TagsParser.class);
    public static final int LINE_ITEMS_COUNT = 6;
    public static final int ID_POSITION = 0;
    public static final int TAGS_POSITION = 1;

    private String id;
    private String tags;

    private boolean success;

    public void parse(String line) {
        init();
        String[] lineItems = line.split("\\t");
        if (lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wrong numbers of items");
            return;
        }

        id = lineItems[ID_POSITION];
        if (StringUtils.isBlank(id)) {
            LOG.warn("id is null in the line {}", line);
            return;
        }

        tags = lineItems[TAGS_POSITION];
        if (StringUtils.isBlank(tags)) {
            LOG.warn("tags is null in the line {}", line);
            return;
        }

        success = true;
    }

    public boolean isFailed() {
        return !success;
    }

    public boolean isSuccess() {
        return success;
    }

    private void init() {
        id = null;
        tags = null;

        success = false;
    }

    public String getId() {
        return id;
    }

    public String getTags() {
        return tags;
    }
}
