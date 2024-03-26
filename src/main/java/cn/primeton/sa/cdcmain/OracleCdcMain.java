package cn.primeton.sa.cdcmain;

import cn.primeton.sa.cdcsink.KafkaSink;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

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
        int opType = parameterTool.getInt("opType");
        String url = parameterTool.get("url");

        DebeziumSourceFunction<String> source = builder.hostname(hostname)
                .url(url)
                .port(port)
                .database(databaseList)
                .schemaList(username)
                .tableList(tableList)
                .username(username)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(getDebeziumProperties())
                .startupOptions(opType == 2 ? StartupOptions.initial() : StartupOptions.latest())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(source)
                // use parallelism 1 for sink to keep message ordering
                .addSink(new KafkaSink()).setParallelism(1);
        env.execute();
    }

    private static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
        properties.setProperty("dateConverters.type", "cn.primeton.sa.cdcmain.OracleDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode", "none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode", "long");
        properties.setProperty("decimal.handling.mode", "double");
        return properties;
    }
}
