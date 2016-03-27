package com.epam.hadoop.hw2.container;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Vitaliy on 3/26/2016.
 */
//TODO try to implement through BufferedReader
public class Splitter implements Iterator<String> {

    public static final int END = -1;
    public static final int START_LINE_LENGTH = 10;
    public static final int NEW_LINE_SCALE = 2;
    public static final byte NEW_LINE = '\n';
    private InputStream inputStream;
    private long offest;
    private long length;
    private boolean skipHeader;
    private long blockPosition;
    private long lines;
    private byte[] line = new byte[START_LINE_LENGTH];
    private int linePosition;
    private boolean blockEnd;

    public Splitter(InputStream inputStream, long offset, long length, boolean skipHeader) throws IOException {
        if(offset < 0) {
            throw new IllegalArgumentException("Offset could not be negative");
        }
        if(length < 0) {
            throw new IllegalArgumentException("Length could not be negative");
        }
        this.inputStream = inputStream;
        this.offest = offset;
        this.length = length;
        long skiped = inputStream.skip(offset);
        if (skiped < offset) {
            throw new IllegalArgumentException("Offset " + offset + " is more then inputStream.");
        }
        blockPosition = offset;
        this.skipHeader = skipHeader;
    }

    @Override
    public boolean hasNext() {
        return !blockEnd;
    }

    @Override
    public String next() {
        if(blockEnd) {
            throw new NoSuchElementException();
        }
        try {
            int byteValue;
            while ((byteValue = inputStream.read()) != END) {
                blockPosition++;
                if(offest != 0 && lines == 0 && byteValue != NEW_LINE) {
                    continue;
                }
                if(byteValue == NEW_LINE) {
                    if(lines == 0 && offest != 0) {
                        lines++;
                        continue;
                    } else if (lines == 0 && offest == 0 && skipHeader) {
                        lines++;
                        linePosition = 0;
                        continue;
                    } else if (blockPosition > offest + length) {
                        blockEnd = true;
                        lines++;
                        String lineString = new String(line, 0, linePosition);
                        linePosition = 0;
                        return lineString;
                    } else {
                        lines++;
                        String lineString = new String(line, 0, linePosition);
                        linePosition = 0;
                        return lineString;
                    }
                }
                addToLine((byte) byteValue);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        blockEnd = true;
        return new String(line, 0, linePosition);
    }

    //TODO implement buffer
    private void addToLine(byte byteVAlue) {
        if(linePosition == line.length - 1) {
            byte[] newLine = new byte[line.length * NEW_LINE_SCALE];
            System.arraycopy(line, 0, newLine, 0, line.length);
            line = newLine;
        }
        line[linePosition] = byteVAlue;
        linePosition++;
    }

}
