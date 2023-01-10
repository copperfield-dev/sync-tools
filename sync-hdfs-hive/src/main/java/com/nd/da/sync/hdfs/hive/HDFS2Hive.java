package com.nd.da.sync.hdfs.hive;

import com.nd.da.sync.hdfs.hive.business.PostRankDO;
import com.nd.da.sync.kits.kit.HdfsKit;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by copperfield @ 2019-03-07 16:17
 */
@Slf4j
public class HDFS2Hive {
    
    public static void main(String[] args) {
        
        String inPath = args[0];
        String outPath = args[1];
        System.out.println("Hello");
        
        HdfsKit hdfsKit = HdfsKit.getHdfsKit();
    
        try {
            FSDataInputStream in = hdfsKit.hdfs.open(new Path(inPath));
            if (!hdfsKit.hdfs.exists(new Path(outPath))) {
                hdfsKit.hdfs.createNewFile(new Path(outPath));
            }
            FSDataOutputStream out = hdfsKit.hdfs.append(new Path(outPath));
            
//            IOUtils.copyBytes(in, System.out, 2048, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            
//            while ((line = reader.readLine()) != null) {
//                System.out.println(PostRankDO.process(line));
//            }
            while ((line = reader.readLine()) != null) {
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                
                for (String cleanedStr : PostRankDO.process(line)) {
//                    System.out.println(cleanedStr);
                    int cleanedLen = cleanedStr.getBytes().length;
                    out.write(cleanedStr.getBytes(), 0, cleanedLen);
                    out.write("\n".getBytes());
                }
            }
            
        } catch (IOException ioe) {
            log.error("Can't open file: {}", ioe);
        }
        
    }
}
