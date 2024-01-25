package cn.primeton.sa.cdcmain;

import cn.primeton.sa.cdcsink.KafkaSink;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author liwj
 * @Date 2023/11/20 11:22
 */
public class OracleCdcMain {
    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("192.168.1.103")
                .port(1521)
                // monitor XE database
                .database("orcl")
                // monitor inventory schema
                .schemaList("di")
                // monitor products table
                .tableList("di.ac_entity")
                .username("flinkuser")
                .password("flinkpw")
                // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(sourceFunction)
                // use parallelism 1 for sink to keep message ordering
                .addSink(new KafkaSink()).setParallelism(1);
        env.execute();
    }
}
