package com.qbit.datax.hook;

import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.qbit.datax.hook.utils.ConfigurationUtils;
import com.qbit.datax.hook.utils.RequestUtils;
import com.qbit.datax.hook.utils.Response;
import com.qbit.datax.hook.utils.ShaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * datax 飞书hook
 *
 * @author litao
 */
public class NotificationHook implements Hook {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationHook.class);

    @Override
    public String getName() {
        return "NotificationHook";
    }

    private void handleSignParams(StringBuilder message, Map<?, ?> params, String prefix) {
        Map<?, ?> data = new TreeMap<>(params);
        for (Object key : data.keySet()) {
            if ("sign".equals(key)) {
                continue;
            }
            Object value = data.get(key);
            if (value == null || value instanceof List || value instanceof Object[]) {
                continue;
            }
            if (value instanceof Map) {
                handleSignParams(message, (Map<?, ?>) value, prefix + key + '.');
            } else {
                message.append(prefix).append(key).append("=").append(value).append("&");
            }
        }
    }

    @Override
    public void invoke(Configuration configuration, Map<String, Number> msg) {
        Map<String, Object> map = configuration.getMap("job.notification");
        if (map == null) {
            LOG.info("No notification configuration, configuration path: job.notification");
            return;
        }
        String url = (String) map.get("url");
        String secret = (String) map.get("secret");
        if (url == null || secret == null) {
            LOG.info("invalid notification configuration");
            return;
        }

        List<String> tables = ConfigurationUtils.getTables(configuration);
        if (tables.size() == 0) {
            LOG.info("tables length is 0");
            return;
        }

        Number number = msg.get("writeSucceedRecords");
        if (number == null) {
            LOG.info("No write succeed records");
            return;
        }
        int succeed = number.intValue();

        Map<String, Object> params = new HashMap<>(4);
        params.put("tables", tables);
        params.put("succeedRecords", succeed);
        long timestamp = System.currentTimeMillis() / 1000;
        params.put("timestamp", timestamp);

        StringBuilder message = new StringBuilder();
        handleSignParams(message, params, "");

        message = new StringBuilder(message.substring(0, message.length() - 1));
        String sign = ShaUtil.encrypt(message.toString(), secret);

        params.put("sign", sign);

        Response response = null;
        int count = 3;
        while (count > 0) {
            response = RequestUtils.doPost(url, JSON.toJSONString(params));
            if (response == null || !response.isSuccessful()) {
                count--;
                if (count > 0) {
                    try {
                        Thread.sleep(10_000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            break;
        }

        if (response == null || !response.isSuccessful()) {
            LOG.info("send notification failed");
            return;
        }
        LOG.info("send notification succeed");
    }

    public static void main(String[] args) {
        NotificationHook hook = new NotificationHook();
        Configuration configuration = Configuration.from("{\"core\": {\"transport\": {\"channel\": {\"speed\": {\"byte\": 10485760 } } } }, \"job\": {\"notification\": {\"url\": \"https://nisuiyi.qbitnetwork.com/notification\", \"secret\": \"6YIJXQkhs9mxOQs+74uIIA==\"}, \"setting\": {\"speed\": {\"byte\": 10485760 }, \"errorLimit\": {\"record\": 0, \"percentage\": 0.02 } }, \"content\": [{\"reader\": {\"name\": \"postgresqlreader\", \"parameter\": {\"username\": \"postgres\", \"password\": \"12345678\", \"isTableMode\": false, \"connection\": [{\"querySql\": [\"select id::varchar from account\"], \"jdbcUrl\": [\"jdbc:postgresql://192.168.146.100:5432/test-sync?currentSchema=public&autoReconnect=true&useUnicode=true&stringtype=unspecified\"] }] } }, \"writer\": {\"name\": \"clickhousewriter\", \"parameter\": {\"batchByteSize\": 134217728, \"batchSize\": 65536, \"column\": [\"id\"], \"connection\": [{\"jdbcUrl\": \"jdbc:clickhouse://47.113.107.105:8123/default\", \"table\": [\"account\"] }], \"dryRun\": false, \"password\": \"B6zbwMGPffHXG3sz\", \"postSql\": [], \"preSql\": [], \"username\": \"default\", \"writeMode\": \"insert\"} } }] } }");
        Map<String, Number> msg = new HashMap<>(16);
        msg.put("writeSucceedRecords", 11);
        msg.put("readSucceedRecords", 10);
        msg.put("totalErrorBytes", 0);
        msg.put("writeSucceedBytes", 130);
        msg.put("byteSpeed", 0);
        msg.put("totalErrorRecords", 0);
        msg.put("recordSpeed", 0);
        msg.put("waitReaderTime", 0);
        msg.put("writeReceivedBytes", 130);
        msg.put("stage", 1);
        msg.put("waitWriterTime", 35300);
        msg.put("percentage", 1.0);
        msg.put("totalReadRecords", 10);
        msg.put("writeReceivedRecords", 11);
        msg.put("readSucceedBytes", 130);
        msg.put("totalReadBytes", 130);

        hook.invoke(configuration, msg);
    }
}
