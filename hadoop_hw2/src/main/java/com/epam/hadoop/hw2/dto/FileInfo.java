package com.epam.hadoop.hw2.dto;

import org.apache.hadoop.fs.Path;

/**
 * Created by root on 3/23/16.
 */
public class FileInfo {

    private Path path;
    private Long length;
    private Long modificationTime;

    public FileInfo(Path path, Long length, Long modificationTime) {
        this.path = path;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(Long modificationTime) {
        this.modificationTime = modificationTime;
    }
}
