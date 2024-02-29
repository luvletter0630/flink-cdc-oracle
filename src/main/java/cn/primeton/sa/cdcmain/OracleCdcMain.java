package cn.primeton.sa.cdcmain;

import cn.primeton.sa.cdcsink.KafkaSink;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author liwj
 * @Date 2023/11/20 11:22
 */
@Slf4j
public class OracleCdcMain {
    public static void main(String[] args) throws Exception {
        OracleSource.Builder<String> builder = OracleSource.builder();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        String databaseList = parameterTool.get("databaseList");
        String tableList = parameterTool.get("tableList");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");
        int initialized = parameterTool.getInt("opType");
        String clusterIp = parameterTool.get("clusterIp");

        DebeziumSourceFunction<String> source = builder.hostname(hostname)
                .port(port)
                .database(databaseList)
                .schemaList(username)
                .tableList(tableList)
                .username(username)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(initialized == 2 ? StartupOptions.initial() : StartupOptions.latest())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(source)
                // use parallelism 1 for sink to keep message ordering
                .addSink(new KafkaSink()).setParallelism(1);
        env.execute();
    }
}
