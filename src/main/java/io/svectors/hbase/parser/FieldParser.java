package io.svectors.hbase.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.svectors.hbase.HBaseSchema.HBaseQualifier;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

/**
 * Created by WLU on 5/12/2017.
 */
class FieldParser {

    public static Pair<String, byte[]> parsePrimitiveField(Field field, Object value) {
        if ( value == null) {
            return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), null);
        }
        Preconditions.checkArgument(field.schema().type().isPrimitive());
        switch (field.schema().type()) {
            case INT8:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Short) value));
            case INT16:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Short) value));
            case INT32:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Integer) value));
            case INT64:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Long) value));
            case FLOAT32:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Float) value));
            case FLOAT64:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Double) value));
            case BOOLEAN:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((Boolean) value));
            case STRING:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), Bytes.toBytes((String) value));
            case BYTES:
                return new Pair<>(new HBaseQualifier(field.name(), field.schema().type()).build(), (byte[]) value);
            default:
                return null;

        }
    }

    static Map<String, byte[]> parsePrimitiveFields(Schema schema, Struct value) {
        Map<String, byte[]> map = Maps.newHashMap();
        if (value != null) {
            schema.fields().forEach(f -> {
                if (f.schema().type().isPrimitive()) {
                    Pair<String, byte[]> p = parsePrimitiveField(f, value.get(f));
                    map.put(p.getFirst(), p.getSecond());
                }
            });
        }
        return map;
    }

    static Map<String, byte[]> parseNonPrimitiveFields(Schema schema, Struct value) {
        Map<String, byte[]> map = Maps.newHashMap();
        schema.fields().forEach(f -> {
            if (!f.schema().type().isPrimitive()) {

            }
        });
        return map;
    }
}
