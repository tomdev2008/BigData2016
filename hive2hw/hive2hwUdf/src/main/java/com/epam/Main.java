package com.epam;

import eu.bitwalker.useragentutils.UserAgent;

public class Main {
    public static void main(String[] args) {
        String s = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729),gzip(gfe),gzip(gfe)";

        UserAgent userAgent = new UserAgent(s);

        String d = userAgent.getOperatingSystem().getDeviceType().getName();

        System.out.println(d);
    }
}
