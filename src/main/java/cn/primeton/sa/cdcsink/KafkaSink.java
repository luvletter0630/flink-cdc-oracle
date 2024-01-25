package cn.primeton.sa.cdcsink;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author liwj
 * @Date 2023/11/20 11:28
 */
public class KafkaSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value);
        JSONObject oracleBinLog = JSONObject.parseObject(value);
        String op = oracleBinLog.getString("op");
        if ("u".equals(op)){
            JSONObject djson = JSONObject.of("after",oracleBinLog.getJSONObject("before"));
            djson.getJSONObject("after").put("mysql_binlog_optype","D");
            System.out.println(djson.toString());
            JSONObject ijson = JSONObject.of("after",oracleBinLog.getJSONObject("after"));
            ijson.getJSONObject("after").put("mysql_binlog_optype","U");
            System.out.println(ijson.toString());
        }else if ("d".equals(op)){
            System.out.println(oracleBinLog.get("before"));
        }else if ("c".equals(op)){
            System.out.println(op);
        }else if ("r".equals(op)){

        }
    }
}
