package com.hon.saas.hbase.parser;

import com.google.common.collect.Maps;
import com.hon.saas.hbase.debezium.Constant;
import org.apache.commons.math3.util.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by WLU on 5/12/2017.
 */
public class DebePostgresEventParser implements EventParser {
    @Override
    public Map<String, byte[]> parseKey(SinkRecord record) throws EventParsingException {
        Schema keySchema = record.keySchema();
        Struct key = (Struct) record.key();
        return FieldParser.parsePrimitiveFields(keySchema, key);
    }




    private Optional<Field> findField(List<Field> fields, String fieldName) {
        return fields.stream().filter(f -> f.name().equals(fieldName)).findAny();
    }

    @Override
    public Map<String, byte[]> parseValue(SinkRecord record) throws EventParsingException {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        List<Field> fields = valueSchema.fields();

        Map<String, byte[]> map = Maps.newHashMap();

        // op=c/d/r/u
        Optional<Field> oP = findField(fields, Constant.OP);
        if (oP.isPresent()) {
            Object opValue = value.get(oP.get());
            Pair<String, byte[]> p = FieldParser.parsePrimitiveField(oP.get(), opValue);
            map.put(p.getFirst(), p.getSecond());
        }

        // before record
        Optional<Field> before = findField(fields, Constant.BEFORE);
        if (before.isPresent()) {
            Struct beforeValue = (Struct) value.get(before.get());
            Map<String, byte[]> beforeMap = FieldParser.parsePrimitiveFields(before.get().schema(), beforeValue);
            map.putAll(beforeMap);
        }

        // after record
        Optional<Field> after = findField(fields, Constant.AFTER);
        if (after.isPresent()) {
            Struct afterValue = (Struct) value.get(after.get());
            Map<String, byte[]> afterMap = FieldParser.parsePrimitiveFields(after.get().schema(), afterValue);
            map.putAll(afterMap);
        }
        return map;
    }
}
