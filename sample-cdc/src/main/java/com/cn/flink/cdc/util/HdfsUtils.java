package com.cn.flink.cdc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class HdfsUtils {

    private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);

    static FileSystem hdfs;

    public static FileSystem fileSystem(String url) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("dfs.client.use.datanode.hostname", "true");
                    conf.set("fs.defaultFS", url);
                    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                    Path path = new Path(url);
//                    URI uri = new URI(url);
//                    hdfs = FileSystem.get(uri, conf, "root");
                    hdfs = path.getFileSystem(conf);
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error("error:", e);
            logger.error("初始化hdfs失败");
        }
        return hdfs;
    }

    public static String getCheckpointUrl(String hdfsUrl, String checkpoint) {
        logger.info("exec method is getCheckpointUrl ");
        FileSystem fileSystem = fileSystem(hdfsUrl);

        FileStatus[] status = new FileStatus[0];
        try {
            if (fileSystem.exists(new Path(checkpoint))) {
                status = fileSystem.listStatus(new Path(checkpoint));
                List<FileStatus> fileStatuses = Arrays.asList(status);
                List<FileStatus> collect = fileStatuses.stream()
                        .sorted(Comparator
                                .comparing(FileStatus::getModificationTime)
                                .reversed())
                        .collect(Collectors.toList());
                for (FileStatus fileStatus : collect) {
                    for (FileStatus fileStatus1 : fileSystem.listStatus(fileStatus.getPath())) {
                        if (fileStatus1.getPath().toString().contains("chk")) {
                            logger.info("从hdfs 获取到的checkpoint路径为：" + fileStatus1.getPath().toString());
                            return fileStatus1.getPath().toString();
                        }
                    }
                }
            }

        } catch (IOException e) {
            logger.error("获取checkpoint失败");
        }
        return null;

    }

}
