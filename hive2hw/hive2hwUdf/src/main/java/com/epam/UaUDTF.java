package com.epam;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class UaUDTF extends GenericUDTF {

    private PrimitiveObjectInspector stringOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 1) {
            throw new UDFArgumentException("takes only one argument");
        }

        if (objectInspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("takes a string as a parameter");
        }

        // input inspectors
        stringOI = (PrimitiveObjectInspector) objectInspectors[0];

        // output inspectors -- an object with two fields!
        List<String> fieldNames = new ArrayList<String>(4);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
        fieldNames.add("type");
        fieldNames.add("family");
        fieldNames.add("name");
        fieldNames.add("device");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        final String userAgentString = stringOI.getPrimitiveJavaObject(objects[0]).toString();

        if (userAgentString == null || userAgentString.isEmpty()) {
            return;
        }

        UserAgent userAgent = new UserAgent(userAgentString);

        Browser browser = userAgent.getBrowser();
        OperatingSystem operatingSystem = userAgent.getOperatingSystem();

        forward(new Object[]{
                browser.getBrowserType().getName(),
                browser.getGroup().name(),
                operatingSystem.getName(),
                operatingSystem.getDeviceType().getName()
        });
    }

    @Override
    public void close() throws HiveException {

    }
}
