package com.nd.da.sync.kits.kit;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created by copperfield @ 2018/9/26 15:30
 */
@Slf4j
public class HttpKit {
    
    public static String get(String url, Map<String, String> headers) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        log.info("URL is {}", url);
        try {
            HttpGet httpGet = new HttpGet(url);
            for (String header : headers.keySet()) {
                log.info("Header Key is {}, and value is {}", header, headers.get(header));
                httpGet.setHeader(header, headers.get(header));
            }
            log.info("Executing request: " + httpGet.getURI());
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                /* 获取响应实体 */
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    return EntityUtils.toString(entity);
                }
            }
        } catch (IOException ioex) {
            log.error(ioex.getMessage());
        } finally {
            /* 关闭连接, 释放资源 */
            try {
                httpClient.close();
            } catch (IOException ioex) {
                log.error(ioex.getMessage());
            }
        }
        
        return "";
    }
}
