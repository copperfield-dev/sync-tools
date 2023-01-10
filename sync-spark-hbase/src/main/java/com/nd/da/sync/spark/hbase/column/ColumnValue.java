package com.nd.da.sync.spark.hbase.column;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by huangyafeng on 2017/9/6.
 */
@Data
@Builder
public class ColumnValue implements Serializable {
    
    /* 列名 */
    private byte[] columnName;
    
    /* rowkey */
    private byte[] rowkey;
    
    /* 列值 */
    private byte[] value;
    
    @Override
    public String toString() {
        return "ColumnValue{" +
                   "columnName=" + Arrays.toString(columnName) +
                   ", rowkey=" + Arrays.toString(rowkey) +
                   ", value=" + Arrays.toString(value) +
                   '}';
    }
}
