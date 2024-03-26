package cn.primeton.sa.cdcsink;

import cn.primeton.sa.es.EsSingleon;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author liwj
 * @Date 2023/11/20 11:28
 */
@Slf4j
public class KafkaSink extends RichSinkFunction<String> {
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private ElasticsearchClient esClinet;
    private static final Map<String, String> timeMap = new HashMap<>();
    @Override
    public void invoke(String value, Context context) {
        JSONObject BinLog = JSONObject.parseObject(value);

        // get the runtime parameters
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String topic = parameters.get("topic");
        String clusterIp = parameters.get("clusterIp");
        // get the operation type
        String op = BinLog.getString("op");
        // 指定日期时间格式
        String jobId = getRuntimeContext().getJobId().toString();

        if ("u".equals(op)) {
            // for update operations, extract the before and after data
            JSONObject djson = setJsonObj("before", BinLog, "D");
            sendToKafka(topic, clusterIp, jobId, djson);
            JSONObject ujson = setJsonObj("after", BinLog, "U");
            sendToKafka(topic, clusterIp, jobId, ujson);
        } else if ("d".equals(op)) {
            // for delete operations, extract the before data
            JSONObject deleteData = setJsonObj("before", BinLog, "D");
            sendToKafka(topic, clusterIp, jobId, deleteData);
        } else if ("c".equals(op)) {
            // for insert operations, extract the after data
            JSONObject insertData = setJsonObj("after", BinLog, "I");
            sendToKafka(topic, clusterIp, jobId, insertData);
        } else if ("r".equals(op)) {
            // for init data operations, extract the after data
            JSONObject initData = setJsonObj("after", BinLog, "I");
            sendToKafka(topic, clusterIp, jobId, initData);
        }

    }

    private static JSONObject setJsonObj(String status, JSONObject mysqlObject, String signature) {
        JSONObject json = mysqlObject.getJSONObject(status);
        json.put("DELETE_SIGN", signature);
        json.put("_DATA_CHANGE_TIME_CDC", LocalDateTime.now().format(dtf));
        return json;
    }

    private void sendToKafka(String topic, String clusterIp, String jobId, JSONObject jsonobj) {
        try {
            String insertJsonStr = JSON.toJSONString(jsonobj);
            log.info("insert value: {}", insertJsonStr);
            KafkaProducer.sendMessage(clusterIp, topic, null, insertJsonStr);
            JSONObject logdata = new JSONObject();
            logdata.put("jobId", jobId);
            logdata.put("time", (DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).format(LocalDateTime.now()));
            logdata.put("msg", insertJsonStr);
            log.info("insert es: {}", logdata.toJSONString());
            insertData(logdata, "flink-job");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean hasIndex(String indexName) {
        BooleanResponse exists = null;
        try {
            esClinet = EsSingleon.getEsClient();
            exists = esClinet.indices().exists(d -> d.index(indexName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exists.value();
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean createIndex(String indexName, String indexJson) {
        try {
            esClinet = EsSingleon.getEsClient();
            ElasticsearchIndicesClient indices = esClinet.indices();

            InputStream inputStream = this.getClass().getResourceAsStream(indexJson);


            CreateIndexRequest req = CreateIndexRequest.of(b -> b
                    .index(indexName)
                    .withJson(inputStream)
            );
            CreateIndexResponse indexResponse = indices.create(req);
        } catch (Exception e) {
            log.error("索引创建失败：{}", e.getMessage());
            throw new RuntimeException("创建索引失败");
        }
        return true;
    }


    public boolean insertData(JSONObject data, String indexName) throws Exception {
        String time = (DateTimeFormatter.BASIC_ISO_DATE).format(LocalDateTime.now());
        if (!StringUtils.isEmpty(time)) {
            String indexTime = time;
            indexName = indexName + "-" + indexTime;
            if (timeMap.get(indexName) == null) {
                timeMap.put(indexName, time);
                if (!hasIndex(indexName)) {
                    createIndex(indexName, "/flinkjobindex.json");
                }
            }

        }
        insertESData(indexName, data);
        return true;
    }

    public void insertESData(String index, JSONObject msg) throws Exception {

        boolean b = batchInsertDocument(index, Collections.singletonList(msg));
    }

    public boolean batchInsertDocument(String indexName, List<JSONObject> objs) throws Exception {
        esClinet = EsSingleon.getEsClient();
        try {
            IndexResponse indexResponse = esClinet.index(
                    x -> x.index(indexName).document(objs.get(0)));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
