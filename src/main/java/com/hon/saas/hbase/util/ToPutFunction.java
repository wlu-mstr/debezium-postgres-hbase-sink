/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hon.saas.hbase.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.hon.saas.hbase.config.HBaseSinkConfig;
import com.hon.saas.hbase.parser.EventParser;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Map;


/**
 * @author ravi.magham
 */
public class ToPutFunction implements Function<SinkRecord, Put> {

    private final HBaseSinkConfig sinkConfig;
    private final EventParser eventParser;

    public ToPutFunction(HBaseSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.eventParser = sinkConfig.eventParser();
    }

    /**
     * Converts the sinkRecord to a {@link Put} instance
     * The event parser parses the key schema of sinkRecord only when there is
     * no property configured for {@link HBaseSinkConfig#TABLE_ROWKEY_COLUMNS_TEMPLATE}
     *
     * @param sinkRecord
     * @return
     */
    @Override
    public Put apply(final SinkRecord sinkRecord) {
        Preconditions.checkNotNull(sinkRecord);
        System.out.println(sinkRecord);
        final String table = sinkRecord.topic();
        final String columnFamily = columnFamily(table);
        final String delimiter = rowkeyDelimiter(table);

        final Map<String, byte[]> valuesMap  = this.eventParser.parseValue(sinkRecord);
        final Map<String, byte[]> keysMap = this.eventParser.parseKey(sinkRecord);

        keysMap.entrySet().forEach(e -> {
            System.out.println(e.getKey() + "  key ---   " + Bytes.toString(e.getValue()));
        });

        valuesMap.entrySet().forEach(e -> {
            System.out.println(e.getKey() + "  value ---   " + Bytes.toString(e.getValue()));
        });

        if (valuesMap.isEmpty() || keysMap.isEmpty()) {
            System.out.println("can't cons a Put");
            return null;
        }
        valuesMap.putAll(keysMap);
        final String[] rowkeyColumns = rowkeyColumns(table);
        final byte[] rowkey = toRowKey(valuesMap, rowkeyColumns, delimiter);

        final Put put = new Put(rowkey);
        valuesMap.entrySet().forEach(entry -> {
            final String qualifier = entry.getKey();
            final byte[] value = entry.getValue();
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
        });
        return put;
    }

    /**
     * A kafka topic is a 1:1 mapping to a HBase table.
     * @param table
     * @return
     */
    private String[] rowkeyColumns(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_COLUMNS_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry);
        return entryValue.split(",");
    }

    /**
     * Returns the delimiter for a table. If nothing is configured in properties,
     * we use the default {@link HBaseSinkConfig#DEFAULT_HBASE_ROWKEY_DELIMITER}
     * @param table hbase table.
     * @return
     */
    private String rowkeyDelimiter(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_DELIMITER_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, HBaseSinkConfig.DEFAULT_HBASE_ROWKEY_DELIMITER);
        return entryValue;
    }

    /**
     * Returns the column family mapped in configuration for the table.  If not present, we use the
     * default {@link HBaseSinkConfig#DEFAULT_HBASE_COLUMN_FAMILY}
     * @param table hbase table.
     * @return
     */
    private String columnFamily(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_COLUMN_FAMILY_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, HBaseSinkConfig.DEFAULT_HBASE_COLUMN_FAMILY);
        return entryValue;
    }

    /**
     *
     * @param valuesMap
     * @param columns
     * @return
     */
    private byte[] toRowKey(final Map<String, byte[]> valuesMap, final String[] columns, final String delimiter) {
        Preconditions.checkNotNull(valuesMap);
        Preconditions.checkNotNull(delimiter);

        byte[] rowkey = null;
        byte[] delimiterBytes = Bytes.toBytes(delimiter);
        for(String column : columns) {
            byte[] columnValue = valuesMap.get(column);
            if(rowkey == null) {
                rowkey = columnValue;
            } else {
                if (null == columnValue) {
                    columnValue = new byte[] {'N','A'};
                }
                rowkey = Bytes.add(rowkey, delimiterBytes, columnValue);
            }
        }
        return rowkey;
    }
}
