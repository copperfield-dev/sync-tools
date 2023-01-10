package com.nd.da.sync.hdfs.hive.business;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

/**
 * Created by copperfield @ 2019-03-14 17:55
 */
public class PostRankDO {
    
    public static List<String> process(String dirtyStr) {
        String[] beautyStrs = new String[10];
        System.out.println(dirtyStr);
        try {
            String[] dirtyStrs = dirtyStr.split("\u0001");
            beautyStrs[0] = dirtyStrs[0];
    
            if (dirtyStrs[1].contains("§")) {
                String[] tmp = dirtyStrs[1].split("§");
                beautyStrs[1] = tmp[0];
        
                if (tmp.length > 1) {
                    beautyStrs[2] = tmp[1];
                } else {
                    beautyStrs[2] = " ";
                }
            } else {
                beautyStrs[1] = dirtyStrs[1];
                beautyStrs[2] = " ";
            }
            
//            System.out.println(dirtyStrs[0] + "," + dirtyStrs[2] + "," + dirtyStrs[3]);
            beautyStrs[3] = new String(Base64.getDecoder().decode(dirtyStrs[3]), "GB2312");
            beautyStrs[4] = dirtyStrs[4];
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(dirtyStr);
        }
//        System.out.println(StringUtils.join(beautyStrs, ","));
//        System.out.println(beautyStrs[3]);
        List<String> cleaned = processXML(beautyStrs);
    
        return cleaned;
    }
    
    private static List<String> processXML(String[] str) {
        SAXReader reader = new SAXReader();
        InputStream in = new ByteArrayInputStream(str[3].getBytes());
        List<String> cleaned = Lists.newArrayList();
        try {
            Document doc = reader.read(in);
            Element root = doc.getRootElement();
            
            Element rows = root.element("Rows");
    
            if (rows != null) {
                Iterator<Element> rowIt = rows.elementIterator();
        
                while (rowIt.hasNext()) {
                    Element element = rowIt.next();
                    List<Element> row = element.elements();
                    String[] result = new String[12];
                    int i = 0;
                    for (Element child : row) {
                        result[i] = child.getText();
                        i++;
                        /* 只取前12列的数据 */
                        if (i > 11) {
                            break;
                        }
                    }
//                    System.out.println(StringUtils.join(str, ",", 0, 2) + ", " + StringUtils.join(result, ","));
                    cleaned.add(StringUtils.join(str, ",", 0, 2) + ", "
                                    + StringUtils.join(result, ",")  + ", "
                                    + str[4]);
                }
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        
        return cleaned;
    }
}
