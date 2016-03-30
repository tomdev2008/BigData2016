package biddings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class Parser {

    private static final Logger LOG = LoggerFactory.getLogger(Parser.class);
    public static final int LINE_ITEMS_COUNT = 22;
    public static final int IP_POSITION = 4;
    public static final int BIDINGS_POSITION = 18;

    private String ip;
    private Long bidings;

    private boolean success = false;

    public void parse(String line) {
        init();
        String[] lineItems = line.split("\t");
        if(lineItems.length != LINE_ITEMS_COUNT) {
            LOG.warn("Wring numbers of items in line " + line);
            return;
        }
        String ip = lineItems[IP_POSITION];
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
        success = false;
    }

    public String getIp() {
        return ip;
    }

    public Long getBidings() {
        return bidings;
    }
}
