package nd.bigdata.engine.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

/**
 * Created by copperfield @ 2019-11-19 19:32
 */
@Slf4j
public class HiveToMySQL {
    
    public static void main(String[] args) {
        
        // if the resources folder does not contain the hive-site.xml file, set the following items
        SparkSession spark = SparkSession.builder()
                                         .appName("hive-to-mysql")
                                         .config("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                                         .config("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName())
                                         .config("hive.metastore.uris", "thrift://192.168.19.25:9083")
//                                         .config("hive.exec.scratchdir", "/tmp/scratchdir")
                                         .enableHiveSupport()
                                         .getOrCreate();
//        spark.sparkContext().setLogLevel("DEBUG");
        SQLContext sqlContext = new SQLContext(spark);
        
        if (args.length < 6) {
            log.info("Database & Table's name are needed");
        }
        
        String hiveDb = args[0];
        String hiveTable = args[1];
        String sql = "SELECT * FROM " + hiveDb + "." + hiveTable;
        
        String mysqlUrl = args[2];
        String mysqlTable = args[3];
        String mysqlUser = args[4];
        String mysqlPassword = args[5];
        
        if (args.length > 6) {
            String mysqlWhere = args[6];
            sql += " ";
            sql += mysqlWhere;
        }
    
        log.info(sql);
        System.out.println(sql);
    
        Dataset<Row> dataset;
        dataset = sqlContext.sql(sql);
        dataset.show(10, false);
        
        dataset.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", mysqlUrl)
                .option("dbtable", mysqlTable)
                .option("user", mysqlUser)
                .option("password", mysqlPassword)
                .save();
        
        spark.close();
    }
}
