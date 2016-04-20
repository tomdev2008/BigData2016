package com.epam;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UaUDF extends UDF {

   public Text evaluate(Text text) {
       if (text == null) {
           return null;
       }

       UserAgent userAgent = new UserAgent(text.toString());
       return new Text(userAgent.getOperatingSystem().getDeviceType().getName());
   }

}
