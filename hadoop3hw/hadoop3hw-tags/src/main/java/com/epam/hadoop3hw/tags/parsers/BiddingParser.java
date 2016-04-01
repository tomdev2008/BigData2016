package com.epam.hadoop3hw.tags.parsers;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class BiddingParser {

    private static final Logger LOG = LoggerFactory.getLogger(BiddingParser.class);
    public static final int LINE_ITEMS_COUNT = 22;
    public static final int USER_TAGS_POSITION = 20;

    private String userTags;

    private boolean success = false;

    public void parse(String line) {
        init();
        String[] lineItems = line.split("\t");
        if(lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wrong numbers of items in line " + line);
            return;
        }

        userTags = lineItems[USER_TAGS_POSITION];
        if(StringUtils.isBlank(userTags)) {
            LOG.warn("User Tags is null in the line {}", line);
            return;
        }

        success = true;
    }

    public boolean isFailed() {
        return !success;
    }

    private void init() {
        userTags = null;

        success = false;
    }

    public String getUserTags() {
        return userTags;
    }

    public boolean isSuccess() {
        return success;
    }
}
