package com.nd.da.sync.kits.kit;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Copperfield @ 2018/8/13 15:32
 */
@Slf4j
public class HdfsKit {
    
    private volatile static HdfsKit hdfsKit;
    
    public FileSystem hdfs;
    
    private HdfsKit() {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static HdfsKit getHdfsKit() {
        if (hdfsKit == null) {
            synchronized (HdfsKit.class) {
                if (hdfsKit == null) {
                    hdfsKit = new HdfsKit();
                }
            }
        }
        return hdfsKit;
    }
    
    public static FileStatus[] getAllFiles(Path path) {
        HdfsKit hdfsKit = getHdfsKit();
        try {
            return hdfsKit.hdfs.globStatus(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
//    private void init(Configuration conf) {
//        try {
//            this.hdfs = FileSystem.get(conf);
//            System.out.println(hdfs.toString());
//        } catch (IOException e) {
//            log.error("Init HDFS Client Failure!", e);
//        }
//    }
    
    public static boolean isExist(Path path) {
        HdfsKit hdfsKit = getHdfsKit();
    
        try {
            return hdfsKit.hdfs.exists(path);
        } catch (IOException e) {
            log.error("Cannot read path!", e);
        }
        return false;
    }
    
    public static boolean isDirectory(Path path) {
        HdfsKit hdfsKit = getHdfsKit();
        
        try {
            return hdfsKit.hdfs.isDirectory(path);
        } catch (IOException ioe) {
            log.error("Cannot read path!", ioe);
        }
        return false;
    }
}
