package com.begor.kc.cassandra;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraSinkConnector extends SinkConnector {

    Map<String, String> settings;
    CassandraSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.config = new CassandraSinkConnectorConfig(props);
        this.settings = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CassandraSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(settings);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;

    }

    @Override
    public ConfigDef config() {
        return CassandraSinkConnectorConfig.config();
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public void stop() {
    }
}
