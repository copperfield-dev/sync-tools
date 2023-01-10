package com.nd.da.sync.spark.hbase.column;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by copperfield @ 26/03/2018 11:45
 */
public class ColumnComparator implements Comparator<ColumnValue>, Serializable {
    /**
     * 字典序比较器
     */
    @Override
    public int compare(ColumnValue o1, ColumnValue o2) {
        return Bytes.compareTo(o1.getColumnName(), o2.getColumnName());
    }
}
