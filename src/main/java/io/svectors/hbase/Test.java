package io.svectors.hbase;

import io.svectors.hbase.parser.DebePostgresEventParser;
import io.svectors.hbase.parser.JsonEventParser;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.net.URL;
import java.net.URLClassLoader;
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
        String value = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"signup_date\"}],\"optional\":true,\"name\":\"exampledb.public.user_tbl.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"signup_date\"}],\"optional\":true,\"name\":\"exampledb.public.user_tbl.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_usec\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"txId\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"lsn\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"snapshot\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"last_snapshot_record\"}],\"optional\":false,\"name\":\"io.debezium.connector.postgresql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"}],\"optional\":false,\"name\":\"exampledb.public.user_tbl.Envelope\",\"version\":1},\"payload\":{\"before\":{\"name\":\"WeiLu0511_1\",\"signup_date\":null},\"after\":null,\"source\":{\"name\":\"exampledb\",\"ts_usec\":1494489067645992,\"txId\":1887,\"lsn\":24836969,\"snapshot\":null,\"last_snapshot_record\":null},\"op\":\"d\",\"ts_ms\":1494489067674}}";
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
        pk.entrySet().forEach(e -> System.out.println(e.getKey() + "   ---  " + Bytes.toString(e.getValue())));

        Map<String, byte[]> pv = p.parseValue(record);
        pv.entrySet().forEach(e -> {
            String k = e.getKey();
            if (k.split("\\|")[1].equals("INT32")) {
                if  (e.getValue() != null)
                System.out.println(e.getKey() + "   ---  " + Bytes.toInt(e.getValue()));
                else
                    System.out.println(e.getKey() + "   ---  " + null);
            }
            else {
                System.out.println(e.getKey() + "   ---  " + Bytes.toString(e.getValue()));
            }
        });
    }



}
