package com.nd.da.sync.spark.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nd.da.sync.spark.hbase.column.ColumnComparator;
import com.nd.da.sync.spark.hbase.column.ColumnValue;
import com.nd.da.sync.spark.hbase.conf.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by copperfield @ 2018/9/19 15:16
 */
@Slf4j
public class HFileGenerateTool {
    
    private transient static Field[] fields = TrackData.class.getDeclaredFields();
    
    public static void main(String[] args) {
        if (args.length < 3) {
            errorReport();
            System.exit(-1);
        }
        log.info("Args is {}", Arrays.toString(args));
        
        final String dateStr = args[0];
        // 列族名称
        final String columnFamily = args[1];
        // HFile 存放目录
        final String tempFolder = args[2];
        // 获取应用名和osType的url
        final String url = args[3];
        // 数据源文件路径
        final String filePathPrefix = args[4];
        String content = args[5];
        String xloginname = args[6];
        String xpassword = args[7];
        
        Map<String, String> headers = Maps.newHashMap();
        headers.put("Content-Type", content);
        headers.put("x-loginname", xloginname);
        headers.put("x-password", xpassword);
        // 并行度
//        final int parallelism = Integer.valueOf(args[4]);
        
        // 配置SparkConf
        SparkConf conf = new SparkConf();
        conf.setAppName(HFileGenerateTool.class.getName());
        // 构建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 把埋点数据类的字段转换成HBase上的列标识符
        String[] columns = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            columns[i] = fields[i].getName();
        }
        
        // 列族和列限定符转化为 byte[]
        final byte[] byteColumnFamily = Bytes.toBytes(columnFamily);
        Map<String, byte[]> columnMap = Maps.newHashMap();
        for (String column : columns) {
            columnMap.put(column, Bytes.toBytes(column));
        }
        
        String hdfsPaths = TrackData.getHDFSFilePath(dateStr, url, filePathPrefix, headers);
        JavaPairRDD<TrackData, Long> dataRDD = sc.textFile(hdfsPaths)
                                                   .map(TrackData::parseString)
                                                   .zipWithUniqueId();
        
        dataRDD.flatMap(obj -> {
            List<ColumnValue> result = Lists.newArrayList();
            Long uid = obj._2;
            TrackData data = obj._1;
            // 生成rowkey
            Field createTimeField = TrackData.class.getDeclaredField("createTime");
            createTimeField.setAccessible(true);
            String createTime = createTimeField.get(data).toString();
            
            Field eventTagField = TrackData.class.getDeclaredField("eventTag");
            eventTagField.setAccessible(true);
            String eventTag = eventTagField.get(data).toString();
            
            Field componentIdentityField = TrackData.class.getDeclaredField("componentIdentity");
            componentIdentityField.setAccessible(true);
            String componentIdentity = componentIdentityField.get(data).toString();
            String id = "";
            if (componentIdentity.equals("app")) {
                Field sdpAppIdField = TrackData.class.getDeclaredField("sdpAppId");
                sdpAppIdField.setAccessible(true);
                String sdpAppId = sdpAppIdField.get(data).toString();
                Field osTypeField = TrackData.class.getDeclaredField("osType");
                osTypeField.setAccessible(true);
                String osType = osTypeField.get(data).toString();
                id = sdpAppId + osType;
            } else {
                id = componentIdentity;
            }
            
            byte[] rowkey = generateRowKey(createTime, id, eventTag, uid);
            
            // 处理非rowkey列的值
            for (String column : columns) {
                if (column.equals("id")
                        || column.equals("sdpAppId")
                        || column.equals("osType")
                        || column.equals("componentIdentity")
                        || column.equals("log")) {
                    continue;
                }
                
                Field field = TrackData.class.getDeclaredField(column);
                field.setAccessible(true);
                byte[] value = Bytes.toBytes(field.get(data).toString());
                
                if (value == null) {
                    continue;
                }
                
                ColumnValue cv = ColumnValue.builder()
                                     .columnName(columnMap.get(column))
                                     .rowkey(rowkey)
                                     .value(value)
                                     .build();
                result.add(cv);
            }
            result.sort(new ColumnComparator());
            return result;
        }).mapToPair(cv -> new Tuple2<>(new ImmutableBytesWritable(cv.getRowkey()),
            new KeyValue(cv.getRowkey(), byteColumnFamily, cv.getColumnName(), cv.getValue())))
            .sortByKey()
            .saveAsNewAPIHadoopFile(tempFolder, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, Config.getInstance());
    }
    
    private static byte[] generateRowKey(String createTime, String id, String eventTag, Long uid) {
        String rowkeyStr = id + "-" + eventTag + "-" + createTime + String.valueOf(uid);
        return Bytes.toBytes(rowkeyStr);
    }
    
    private static void errorReport() {
        System.err.println("Args must be = {columnFamily} {tempFolder}");
    }
}
