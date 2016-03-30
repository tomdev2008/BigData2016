package com.epam.hadoop3hw.biddings;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);
    public static final int LINE_ITEMS_COUNT = 22;
    public static final int USER_AGENT_POSITION = 2;
    public static final int IP_POSITION = 4;
    public static final int BIDINGS_POSITION = 18;

    private String ip;
    private Long bidings;
    private String browser;

    private boolean success = false;

    public void parse(String line) {
        init();
        String[] lineItems = line.split("\t");
        if(lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wring numbers of items in line " + line);
            return;
        }

        UserAgent userAgent = new UserAgent(lineItems[USER_AGENT_POSITION]);
        browser = userAgent.getBrowser().getGroup().name();

        ip = lineItems[IP_POSITION];
        if(ip == null) {
            LOG.warn("ip is nul in the line {}", line);
            return;
        }
        String bidingsItem = null;
        try {
            bidingsItem = lineItems[BIDINGS_POSITION];
            bidings = Long.parseLong(bidingsItem);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse biding {} in line {}", bidingsItem, line);
            return;
        }
        success = true;
    }

    public boolean isFailed() {
        return !success;
    }

    private void init() {
        ip = null;
        bidings = null;
        browser = null;

        success = false;
    }

    public String getIp() {
        return ip;
    }

    public Long getBidings() {
        return bidings;
    }

    public String getBrowser() {
        return browser;
    }
}
