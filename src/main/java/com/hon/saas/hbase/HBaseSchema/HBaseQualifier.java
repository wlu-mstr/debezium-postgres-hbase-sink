package com.hon.saas.hbase.HBaseSchema;

import org.apache.kafka.connect.data.Schema;

/**
 * Created by WLU on 5/12/2017.
 */
public class HBaseQualifier {
    private final String columnName;
    private final Schema.Type dataType;

    public HBaseQualifier(String columnName, Schema.Type dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    public String build() {
        return columnName + "|" + dataType;
    }
}
