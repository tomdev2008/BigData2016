package com.epam.hadoop.hw2;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created by root on 3/23/16.
 */
public class Utils {

    private static FileStatus copyToHdfs(FileSystem fs, String fileSrcPath,
                           String fileDstPath, String appId, Map<String, LocalResource> localResources,
                           String resources, String appName, Path dst) throws IOException {


        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        return fs.getFileStatus(dst);
    }

    static FileInfo copyAndAddToLocalResources(FileSystem fs, String fileSrcPath,
                                           String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                           String resources, String appName) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);

        System.out.println("dst = " + dst);

        FileStatus fileStatus = copyToHdfs(fs, fileSrcPath, fileDstPath, appId, localResources, resources, appName, dst);

        addToLocalResources(localResources, dst, fileDstPath, fileStatus.getLen(), fileStatus.getModificationTime());

        return new FileInfo(dst, fileStatus.getLen(), fileStatus.getModificationTime());
    }

    static void addToLocalResources(Map<String, LocalResource> localResources, Path filePath, String fileDstPath, long len, long modificationTime) throws IOException {

        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(filePath.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        len, modificationTime);
        localResources.put(fileDstPath, scRsrc);
    }

}
