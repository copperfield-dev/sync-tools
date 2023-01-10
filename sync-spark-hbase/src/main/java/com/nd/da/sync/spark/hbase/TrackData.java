package com.nd.da.sync.spark.hbase;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nd.da.sync.kits.kit.HdfsKit;
import com.nd.da.sync.kits.kit.HttpKit;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;

/**
 * Created by copperfield @ 2018/9/19 17:32
 */
@Data
@Builder
@Slf4j
public class TrackData {
    
    private String id;
    private String sdpAppId;
    private String osType;
    private String componentIdentity;
    private String eventTag;
    private String date;
    private String eventVersion;
    private String appVersion;
    private String deviceId;
    private String channelId;
    private String userId;
    private String levelOneArea;
    private String levelTwoArea;
    private String levelThreeArea;
    private String ip;
    private String data;
    private String createTime;
    
    public static String getHDFSFilePath(String dateStr, String url, String filePathPrefix, Map<String, String> headers) {
        String jsonStr = HttpKit.get(url, headers);
        JsonObject jsonObject = new JsonParser().parse(jsonStr).getAsJsonObject();
        int count = jsonObject.get("count").getAsInt();
        
        JsonArray items = jsonObject.get("items").getAsJsonArray();
        
        List<String> hdfsPath = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            JsonObject object = items.get(i).getAsJsonObject();
            String sdpAppId = object.get("sdp_app_id").getAsString();
            String osType = object.get("os_type").getAsString();
            String path = filePathPrefix + sdpAppId + "/" + osType;
            FileStatus[] status = HdfsKit.getAllFiles(new Path(path + "/*/" + dateStr + "/*"));
            
            if (status != null) {
                for (FileStatus fs : status) {
                    if (HdfsKit.isExist(fs.getPath())) {
                        hdfsPath.add(fs.getPath().toString());
                    }
                }
            }
        }
        
        log.info("HDFS Path is: " + hdfsPath);
        return StringUtils.join(hdfsPath, ",");
    }
    
    public static TrackData parseString(String message) {
        String[] strs = message.split("\u0001");
        return TrackData.builder()
                   .id(strs[0])
                   .sdpAppId(strs[1])
                   .osType(strs[2])
                   .componentIdentity(strs[3])
                   .eventTag(strs[4])
                   .date(strs[5])
                   .eventVersion(strs[6])
                   .appVersion(strs[7])
                   .deviceId(strs[8])
                   .channelId(strs[9])
                   .userId(strs[10])
                   .levelOneArea(strs[11])
                   .levelTwoArea(strs[12])
                   .levelThreeArea(strs[13])
                   .ip(strs[14])
                   .data(strs[15])
                   .createTime(strs[16])
                   .build();
    }
}
