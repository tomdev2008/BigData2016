package com.epam.hadoop.hw2.container;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 3/24/16.
 */
public class InputLinkLine {

    public static final int LINK_POSITION = 5;

    private ArrayList<String> lineItems;

    public InputLinkLine(ArrayList lineItems) {
        this.lineItems = lineItems;
    }

    public ArrayList<String> getLineItems() {
        return new ArrayList<>(lineItems);
    }

    public String getLink() {
        return lineItems.get(LINK_POSITION);
    }

}
