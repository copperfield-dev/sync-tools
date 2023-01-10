package com.nd.da.sync.spark.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;

/**
 * Created by copperfield @ 2018/9/19 15:02
 */
public class Config {
    private static Configuration conf;
    
    public static synchronized Configuration getInstance() {
        if (conf == null) {
            conf = new Configuration();
//            conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);//设置读块缓存命中。
            
            conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.SnappyCodec");
            // compress map output
            conf.set("mapred.compress.map.output", "true");
            conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            
            conf.set("mapred.output.compress", "true");
            conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            conf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
//            conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName());
        }
        return conf;
    }
}
