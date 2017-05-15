package com.hon.saas.hbase.debezium;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WLU on 5/14/2017.
 * Data structure of debezium-postgres event message
 */
public class DecoderBufsMessage {
    private static final String OP = "op|STRING";
    private static final String TS_MS = "ts_ms|INT64";

    private byte[] opString; // OP
    private Map<String, byte[]> before = new HashMap<>();
    private Map<String, byte[]> after = new HashMap<>();
    private byte[] tsMsLong; // time stamp

    public byte[] getOpString() {
        return opString;
    }

    public void setOpString(byte[] opString) {
        this.opString = opString;
    }

    public Map<String, byte[]> getBefore() {
        return before;
    }

    public void setBefore(Map<String, byte[]> before) {
        this.before = before;
    }

    public Map<String, byte[]> getAfter() {
        return after;
    }

    public void setAfter(Map<String, byte[]> after) {
        this.after = after;
    }


    public void setTsMsLong(byte[] tsMsLong) {
        this.tsMsLong = tsMsLong;
    }

    /**
     * create a value map from the message data
     */
    public Map<String, byte[]> valueMap() {
        String opStr = Bytes.toString(opString);
        Map<String, byte[]> map = new HashMap<>();
        map.put(OP, opString);
        map.put(TS_MS, tsMsLong);
        if (opStr.equals("d")) {
            return map;
        } else {
            map.putAll(after);
        }
        return map;
    }
}
