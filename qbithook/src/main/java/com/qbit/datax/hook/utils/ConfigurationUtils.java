package com.qbit.datax.hook.utils;

import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author litao
 */
public class ConfigurationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

    public static List<String> getTablesByConnection(Map<String, Object> connection) {
        if (connection == null) {
            return new ArrayList<>();
        }
        List<String> origin = (List<String>) connection.get("table");
        if (origin == null) {
            return new ArrayList<>();
        }
        return origin;
    }

    public static List<String> getTablesByContent(Map<String, Object> content) {
        List<String> tables = new ArrayList<>();
        if (content == null) {
            return tables;
        }
        Map<String, Object> writer = (Map<String, Object>) content.get("writer");
        if (writer == null) {
            return tables;
        }
        Map<String, Object> parameter = (Map<String, Object>) writer.get("parameter");

        String table = (String) parameter.get("table");
        if (table != null && !table.isEmpty()) {
            tables.add(table);
        }

        List<Object> connections = (List<Object>) parameter.get("connection");
        if (connections == null || connections.size() == 0) {
            return tables;
        }
        for (Object connection : connections) {
            List<String> ret = getTablesByConnection((Map<String, Object>) connection);
            tables.addAll(ret);
        }
        return tables;
    }

    public static List<String> getTables(Configuration configuration) {
        List<String> tables = new ArrayList<>();
        List<Object> contents = configuration.getList("job.content");
        if (contents == null || contents.size() == 0) {
            LOG.info("no job content");
            return tables;
        }
        for (Object content : contents) {
            List<String> ret = getTablesByContent((Map<String, Object>) content);
            tables.addAll(ret);
        }
        return tables;
    }
}
