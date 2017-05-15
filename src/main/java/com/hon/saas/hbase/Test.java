package com.hon.saas.hbase;

import com.hon.saas.hbase.parser.DebePostgresEventParser;
import com.hon.saas.hbase.parser.JsonEventParser;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.util.HashMap;
import java.util.Map;
//import org.apache.kafka.connect.storage.Converter;


/**
 * Created by H224441 on 5/12/2017.
 */
public class Test {
    public static void main(String[] args) {

        Map<String, String> props = new HashMap<>(1);
        props.put("schemas.enable", Boolean.TRUE.toString());

        //////
        String value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"signup_date\"}],\"optional\":true,\"name\":\"exampledb.public.user_tbl.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"signup_date\"}],\"optional\":true,\"name\":\"exampledb.public.user_tbl.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_usec\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"snapshot\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"last_snapshot_record\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}],\"optional\":false,\"name\":\"exampledb.public.user_tbl.Envelope\",\"version\":1},\"payload\":{\"before\":null,\"after\":{\"name\":\"WeiLu0511_1\",\"signup_date\":16061},\"source\":{\"name\":\"exampledb\",\"ts_usec\":1494488768585609,\"txId\":1886,\"lsn\":24834940,\"snapshot\":null,\"last_snapshot_record\":null},\"op\":\"c\",\"ts_ms\":1494488768626}}";
        Converter jsonC = new JsonConverter();
        jsonC.configure(props, false);
        SchemaAndValue valueAndSchema = jsonC.toConnectData("dummy topic", Bytes.toBytes(value));
        //System.out.println("value:  " + valueAndSchema.toString());

        ///////
        String key = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"}],\"optional\":false,\"name\":\"exampledb.public.user_tbl.Key\"},\"payload\":{\"name\":\"WeiLu0511_1\"}}";
        Converter jsonK = new JsonConverter();
        jsonK.configure(props, true);
        SchemaAndValue keyAndSchema = jsonK.toConnectData("dummy topic", Bytes.toBytes(key));
//        System.out.println("key:  " + keyAndSchema.toString());

        SinkRecord record = new SinkRecord("dummy topic", 1,
                keyAndSchema.schema(), keyAndSchema.value(),
                valueAndSchema.schema(), valueAndSchema.value(),
                100);

//        System.out.println("record:  " + record);

        ///////  test kafka-hbase
        JsonEventParser parser = new JsonEventParser();

        Integer pt = record.kafkaPartition();
        System.out.println("Partition -- " + pt);

        long ofst = record.kafkaOffset();
        System.out.println("Offset -- " + ofst);


        DebePostgresEventParser p = new DebePostgresEventParser();
        Map<String, byte[]> pk = p.parseKey(record);
        pk.entrySet().forEach(e -> System.out.println(e.getKey() + "   --- key --->  " + Bytes.toString(e.getValue())));

        Map<String, byte[]> pv = p.parseValue(record);
        pv.entrySet().forEach(e -> {
            String k = e.getKey();
            if (k.split("\\|")[1].equals("STRING")) {
                System.out.println(k + "  --- value --->   " + Bytes.toString(e.getValue()));
            } else if (k.split("\\|")[1].equals("INT64")) {
                System.out.println(k + "  --- value --->   " + Bytes.toLong(e.getValue()));
            } else if (k.split("\\|")[1].equals("INT32")){
                System.out.println(k + "  --- value --->   " + Bytes.toInt(e.getValue()));
            }
//            if (k.split("\\|")[1].equals("INT32")) {
//                if  (e.getValue() != null)
//                System.out.println(e.getKey() + "   ---  " + Bytes.toInt(e.getValue()));
//                else
//                    System.out.println(e.getKey() + "   ---  " + null);
//            }
//            else {
//                System.out.println(e.getKey() + "   ---  " + Bytes.toString(e.getValue()));
//            }
        });
    }


}
